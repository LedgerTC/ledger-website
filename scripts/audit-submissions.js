#!/usr/bin/env node
// Ledger Trade & Capital — Form Submission Audit
//
// Reconciles three HubSpot data layers over a given date range:
//   1. Contacts created/updated with form_source set  (touched by submit-form.js)
//   2. Tickets created with form_source               (1:1 with successful submissions)
//   3. HubSpot Form submissions (Forms API)           (captured by HS tracking script)
//
// Joins by email + time window and flags mismatches:
//   - HS form submission with no matching ticket          (dual-capture / function failed)
//   - Contact with form_source but no ticket              (function crashed mid-flow)
//   - Multiple form submissions for same email in <5 min  (dual-capture bug)
//
// Posts a summary + mismatch list to a Slack channel via webhook.
//
// Usage:
//   node --env-file=.env scripts/audit-submissions.js                  # all-time (since 2024-01-01)
//   node --env-file=.env scripts/audit-submissions.js --from 2026-05-17 --to 2026-05-24
//   node --env-file=.env scripts/audit-submissions.js --dry-run        # stdout only, no Slack
//   node --env-file=.env scripts/audit-submissions.js --weekly         # last 7 days
//
// Env vars:
//   HUBSPOT_TOKEN          — required. Private app token (Contacts, Tickets, Forms scopes)
//   HUBSPOT_PORTAL_ID      — required. For record URLs in Slack output
//   SLACK_WEBHOOK_URL      — required (unless --dry-run). Incoming webhook for posting the report
//   SLACK_BOT_TOKEN        — optional. Bot token with channels:history scope; enables Layer 5
//                            (counts "submit-form ERROR" alerts posted by submit-form.js)
//   SLACK_CHANNEL_ID       — optional. Channel ID for #nervo_ops (e.g. C0XXXXXXXXX)
//   SLACK_INBOUNDS_CHANNEL_ID — optional. Channel ID for #inbounds. Enables a tab
//                            with automated lead-assignment notifications.
//   CALLRAIL_API_KEY       — optional. Enables Layer 4: CallRail calls vs HubSpot reconciliation
//   CALLRAIL_ACCOUNT_ID    — optional. CallRail account ID

const HUBSPOT_API = "https://api.hubapi.com";

// ─── CallRail forwarding-number → salesperson map ──────────────────
// CallRail's agent_email field is empty for all calls in this account
// (per-agent identity isn't tracked at the CallRail level). The next-best
// signal is which forwarding number picked up the call (business_phone_number).
// This map associates those numbers with the people they belong to.
// Last 10 digits are used as the lookup key, so any format is accepted.
const CALLRAIL_PHONE_TO_NAME = {
  "7327409148": "Tyler",
  "7036557557": "Russell",
  "9728329317": "Umang",
  "9737275591": "Greg",
};
function normalizePhone(p) {
  return String(p || "").replace(/\D/g, "").slice(-10);
}
function phoneToName(p) {
  return CALLRAIL_PHONE_TO_NAME[normalizePhone(p)] || "";
}

// ─── CLI arg parsing ────────────────────────────────────────────────
function parseArgs(argv) {
  const args = { dryRun: false, weekly: false, from: null, to: null, xlsx: null, xlsxDetailed: false, detailedSlack: false, slackChannel: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") args.dryRun = true;
    else if (a === "--weekly") args.weekly = true;
    else if (a === "--from") args.from = argv[++i];
    else if (a === "--to") args.to = argv[++i];
    else if (a === "--slack-channel") args.slackChannel = argv[++i];
    else if (a === "--xlsx") {
      // Optional path; if next arg starts with "--" or is missing, use default
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) { args.xlsx = next; i++; }
      else { args.xlsx = ""; }
    }
    else if (a === "--xlsx-detailed") {
      // Same as --xlsx but uses the legacy 11-tab format
      args.xlsxDetailed = true;
      args.xlsx = args.xlsx === null ? "" : args.xlsx;
    }
    else if (a === "--detailed-slack") args.detailedSlack = true;
    else if (a === "--help" || a === "-h") {
      console.log("Usage: node scripts/audit-submissions.js [options]");
      console.log("  --from YYYY-MM-DD        Start date (default: 2024-01-01)");
      console.log("  --to   YYYY-MM-DD        End date (default: today)");
      console.log("  --weekly                 Convenience: last 7 days");
      console.log("  --dry-run                Print Slack payload to stdout, do not post");
      console.log("  --xlsx [path]            Generate the lightweight 2-tab XLSX (Allocation + Issues)");
      console.log("  --xlsx-detailed [path]   Generate the legacy multi-tab XLSX (for deep dives)");
      console.log("  --detailed-slack         Use legacy multi-section Slack format (default = concise one-pager)");
      console.log("  --slack-channel <id>     Post to a specific channel via bot token instead of SLACK_WEBHOOK_URL");
      process.exit(0);
    }
  }

  const today = new Date().toISOString().slice(0, 10);
  if (args.weekly) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - 7);
    args.from = d.toISOString().slice(0, 10);
    args.to = today;
  }
  if (!args.from) args.from = "2024-01-01";
  if (!args.to) args.to = today;
  return args;
}

// ─── HubSpot API helper ────────────────────────────────────────────
async function hubspot(method, path, body) {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) throw new Error("HUBSPOT_TOKEN env var is required");

  const opts = {
    method,
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(`${HUBSPOT_API}${path}`, opts);
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  if (!res.ok) {
    throw new Error(`HubSpot ${method} ${path} → ${res.status}: ${typeof data === "string" ? data : JSON.stringify(data)}`);
  }
  return data;
}

// ─── Paginated search ──────────────────────────────────────────────
async function searchAll(objectType, filters, properties) {
  const results = [];
  let after = undefined;
  let pages = 0;
  while (pages < 200) {  // safety cap: 200 pages × 100 = 20k records
    const body = {
      filterGroups: [{ filters }],
      properties,
      limit: 100,
      sorts: [{ propertyName: "createdate", direction: "ASCENDING" }],
    };
    if (after) body.after = after;
    const page = await hubspot("POST", `/crm/v3/objects/${objectType}/search`, body);
    if (page.results && page.results.length) results.push(...page.results);
    if (page.paging && page.paging.next && page.paging.next.after) {
      after = page.paging.next.after;
      pages++;
    } else {
      break;
    }
  }
  return results;
}

// ─── HubSpot Owners (id → name/email map) ─────────────────────────
// Resolves hubspot_owner_id on tickets/contacts into human-readable names
// for the Salesperson Allocation tab. Skips gracefully if scope is missing.
async function fetchOwners() {
  const map = new Map();
  let after = undefined;
  let pages = 0;
  while (pages < 20) {
    const qs = new URLSearchParams({ limit: "100" });
    if (after) qs.set("after", after);
    let page;
    try {
      page = await hubspot("GET", `/crm/v3/owners?${qs.toString()}`);
    } catch (err) {
      console.warn(`Could not fetch owners (${err.message}). Salesperson tab will show IDs only.`);
      return { ownersById: map, accessible: false };
    }
    for (const o of (page.results || [])) {
      const fullName = [o.firstName, o.lastName].filter(Boolean).join(" ") || o.email || `Owner ${o.id}`;
      map.set(String(o.id), { id: o.id, email: (o.email || "").toLowerCase(), firstName: o.firstName, lastName: o.lastName, fullName });
    }
    if (page.paging && page.paging.next && page.paging.next.after) {
      after = page.paging.next.after;
      pages++;
    } else {
      break;
    }
  }
  return { ownersById: map, accessible: true };
}

// ─── Layer 1: Contacts created with form_source set ────────────────
async function fetchContacts(fromIso, toIso) {
  const filters = [
    { propertyName: "createdate", operator: "GTE", value: fromIso },
    { propertyName: "createdate", operator: "LTE", value: toIso },
    { propertyName: "form_source", operator: "HAS_PROPERTY" },
  ];
  return searchAll("contacts", filters, [
    "email", "firstname", "lastname", "phone", "form_source", "ad_campaign",
    "createdate", "hubspot_owner_id",
    // Attribution properties — surfaced in the Webform Attribution tab
    "hs_analytics_source", "hs_analytics_source_data_1", "hs_analytics_source_data_2",
    "hs_google_click_id", "gad_campaignid",
  ]);
}

// ─── Layer 2: Tickets created via submit-form.js ───────────────────
async function fetchTickets(fromIso, toIso) {
  const filters = [
    { propertyName: "createdate", operator: "GTE", value: fromIso },
    { propertyName: "createdate", operator: "LTE", value: toIso },
  ];
  const tickets = await searchAll("tickets", filters, [
    "subject", "hs_pipeline", "hs_pipeline_stage", "hs_ticket_category",
    "hs_ticket_priority", "createdate", "hubspot_owner_id",
  ]);

  // Fetch associated contact for each ticket (one round-trip per ticket).
  // Also pulls hubspot_owner_id so we can compare contact-owner vs ticket-owner.
  const enriched = [];
  for (const t of tickets) {
    const assocs = await hubspot("GET", `/crm/v4/objects/tickets/${t.id}/associations/contacts`);
    const contactId = assocs.results && assocs.results[0] ? assocs.results[0].toObjectId : null;
    let email = null;
    let contactOwnerId = null;
    if (contactId) {
      try {
        const c = await hubspot("GET", `/crm/v3/objects/contacts/${contactId}?properties=email,hubspot_owner_id`);
        email = c.properties && c.properties.email ? c.properties.email.toLowerCase() : null;
        contactOwnerId = c.properties && c.properties.hubspot_owner_id ? c.properties.hubspot_owner_id : null;
      } catch { /* ignore — contact may have been deleted */ }
    }
    enriched.push({ ...t, _contactId: contactId, _email: email, _contactOwnerId: contactOwnerId });
  }
  return enriched;
}

// ─── Layer 3: HubSpot Form submissions (Forms API) ─────────────────
// These are captured by the HubSpot tracking script independently of our
// Netlify function. Discrepancies vs Layer 2 surface the dual-capture issue.
async function fetchFormSubmissions(fromIso, toIso) {
  // List all forms in portal
  let forms;
  try {
    forms = await hubspot("GET", "/marketing/v3/forms?limit=200");
  } catch (err) {
    console.warn(`Could not list HubSpot forms (${err.message}). Skipping Layer 3.`);
    return { submissions: [], formsScanned: 0, accessible: false };
  }

  const fromMs = new Date(fromIso).getTime();
  const toMs = new Date(toIso).getTime();
  const all = [];

  for (const form of forms.results || []) {
    const formId = form.id;
    const formName = form.name || form.id;
    // Legacy form-integrations endpoint paginates with `after` cursor (epoch ms)
    let after = undefined;
    let pages = 0;
    while (pages < 50) {
      const qs = new URLSearchParams({ limit: "50" });
      if (after) qs.set("after", after);
      let page;
      try {
        page = await hubspot("GET", `/form-integrations/v1/submissions/forms/${formId}?${qs.toString()}`);
      } catch (err) {
        // Some forms (especially newer ones) aren't queryable via legacy endpoint
        break;
      }
      const subs = page.results || [];
      // Submissions come newest-first; stop once we're past the from window
      let pastWindow = false;
      for (const s of subs) {
        if (s.submittedAt < fromMs) { pastWindow = true; continue; }
        if (s.submittedAt > toMs) continue;
        const emailField = (s.values || []).find(v => v.name === "email");
        all.push({
          formId,
          formName,
          submittedAt: new Date(s.submittedAt).toISOString(),
          email: emailField ? String(emailField.value).toLowerCase() : null,
          pageUrl: s.pageUrl || "",
        });
      }
      if (pastWindow) break;
      if (page.paging && page.paging.next && page.paging.next.after) {
        after = page.paging.next.after;
        pages++;
      } else {
        break;
      }
    }
  }
  return { submissions: all, formsScanned: (forms.results || []).length, accessible: true };
}

// ─── Layer 4: CallRail calls (Calls API) ───────────────────────────
// Pulls the source-of-truth list of calls actually received in the window so
// we can compare against HubSpot "Inbound Call" tickets created by CallRail's
// HubSpot integration.
async function fetchCallRailCalls(fromIso, toIso) {
  const apiKey = process.env.CALLRAIL_API_KEY;
  const accountId = process.env.CALLRAIL_ACCOUNT_ID;
  if (!apiKey || !accountId) {
    console.warn("CALLRAIL_API_KEY/CALLRAIL_ACCOUNT_ID not set — skipping CallRail layer.");
    return { calls: [], accessible: false };
  }

  const startDate = fromIso.slice(0, 10);  // YYYY-MM-DD
  const endDate = toIso.slice(0, 10);
  const all = [];
  let page = 1;
  while (page < 50) {  // safety cap: 50 pages × 250 = 12.5k calls
    const qs = new URLSearchParams({
      start_date: startDate,
      end_date: endDate,
      per_page: "250",
      page: String(page),
      fields: "source,source_name,medium,referrer_domain,landing_page_url,keywords,tracker_id,recording_player,agent_email,note,voicemail",
    });
    const url = `https://api.callrail.com/v3/a/${accountId}/calls.json?${qs.toString()}`;
    let res;
    try {
      res = await fetch(url, {
        headers: { "Authorization": `Token token="${apiKey}"` },
      });
    } catch (err) {
      console.warn(`CallRail fetch failed (${err.message}). Returning partial data.`);
      break;
    }
    if (!res.ok) {
      const text = await res.text();
      console.warn(`CallRail API ${res.status}: ${text.slice(0, 200)}`);
      return { calls: all, accessible: false };
    }
    const data = await res.json();
    const calls = data.calls || [];
    all.push(...calls.map(c => ({
      id: c.id,
      startTime: c.start_time,
      answered: c.answered,
      direction: c.direction,
      duration: c.duration,
      customerPhone: c.customer_phone_number || "",
      customerName: c.customer_name || "",
      customerCity: c.customer_city || "",
      customerState: c.customer_state || "",
      trackingPhone: c.tracking_phone_number || "",
      businessPhone: c.business_phone_number || "",   // forwarded-to number — closest signal to "who picked up"
      source: c.source || "",
      sourceName: c.source_name || "",
      medium: c.medium || "",
      referrerDomain: c.referrer_domain || "",
      landingPageUrl: c.landing_page_url || "",
      keywords: c.keywords || "",
      trackerId: c.tracker_id || "",
      recordingPlayer: c.recording_player || "",
      agentEmail: (c.agent_email || "").toLowerCase(),
      note: c.note || "",
      voicemail: !!c.voicemail,
    })));
    if (calls.length < 250) break;
    page++;
  }
  return { calls: all, accessible: true };
}

// ─── Slack user resolver (id → display name) ──────────────────────
// Cached lookup so we don't refetch the same user across many messages.
const _slackUserCache = new Map();
async function resolveSlackUser(id) {
  if (!id) return id;
  if (_slackUserCache.has(id)) return _slackUserCache.get(id);
  const token = process.env.SLACK_BOT_TOKEN;
  if (!token) { _slackUserCache.set(id, `@${id}`); return `@${id}`; }
  try {
    const r = await fetch(`https://slack.com/api/users.info?user=${id}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await r.json();
    if (data.ok && data.user) {
      const u = data.user;
      const name = u.real_name || (u.profile && u.profile.real_name) || u.name || `@${id}`;
      _slackUserCache.set(id, name);
      return name;
    }
  } catch { /* ignore */ }
  _slackUserCache.set(id, `@${id}`);
  return `@${id}`;
}

// ─── #inbounds Slack channel log ──────────────────────────────────
// Reads the automated notifications HubSpot/CallRail workflows post to
// #inbounds. Two formats are parsed:
//   1. CallRail bot:     "Inbound call from <name> → <assignee>"
//   2. HubSpot bot:      ":fire: *New inbound*\nAssigned to: <@SLACK_ID>\n```...```"
// Returns a flat list of structured entries so we can render a tab + cross-
// reference against HubSpot data.
async function fetchInboundsLog(fromIso, toIso) {
  const token = process.env.SLACK_BOT_TOKEN;
  const channel = process.env.SLACK_INBOUNDS_CHANNEL_ID;
  if (!token || !channel) {
    console.warn("SLACK_BOT_TOKEN/SLACK_INBOUNDS_CHANNEL_ID not set — skipping #inbounds layer.");
    return { entries: [], accessible: false };
  }
  const oldest = Math.floor(new Date(fromIso).getTime() / 1000).toString();
  const latest = Math.floor(new Date(toIso).getTime() / 1000).toString();
  const all = [];
  let cursor = "";
  let pages = 0;
  while (pages < 20) {
    const qs = new URLSearchParams({ channel, oldest, latest, limit: "200" });
    if (cursor) qs.set("cursor", cursor);
    const r = await fetch(`https://slack.com/api/conversations.history?${qs.toString()}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await r.json();
    if (!data.ok) {
      console.warn(`#inbounds Slack API error: ${data.error}. Skipping layer.`);
      return { entries: all, accessible: false };
    }
    for (const m of (data.messages || [])) {
      all.push(parseInboundsMessage(m));
    }
    if (data.has_more && data.response_metadata && data.response_metadata.next_cursor) {
      cursor = data.response_metadata.next_cursor;
      pages++;
    } else {
      break;
    }
  }
  // Slack returns newest-first; reverse for chronological tab display
  all.reverse();

  // Resolve any @USERID mentions in assignee to real names (cached)
  const idPattern = /@([UW][A-Z0-9]+)/g;
  for (const e of all) {
    if (!e.assignee || !idPattern.test(e.assignee)) continue;
    idPattern.lastIndex = 0;
    const ids = [...e.assignee.matchAll(idPattern)].map(m => m[1]);
    for (const id of ids) {
      const name = await resolveSlackUser(id);
      e.assignee = e.assignee.replace(`@${id}`, name);
    }
  }

  return { entries: all, accessible: true };
}

function parseInboundsMessage(m) {
  const text = m.text || "";
  const isoTime = new Date(parseFloat(m.ts) * 1000).toISOString();
  const entry = {
    ts: m.ts, isoTime, type: "other",
    customerName: "", assignee: "", email: "", phone: "",
    channel: "", page: "", keyword: "", rawText: text,
  };

  // CallRail bot: "Inbound call from <name> → <assignee>"
  const callMatch = text.match(/^Inbound call from (.+?) → (.+?)$/m);
  if (callMatch) {
    entry.type = "call";
    entry.customerName = callMatch[1].trim();
    entry.assignee = callMatch[2].trim();
    return entry;
  }

  // HubSpot bot: ":fire: *New inbound*"
  if (text.includes("New inbound") || text.startsWith(":fire:")) {
    entry.type = "form";
    const grabLine = (label) => {
      const re = new RegExp(`${label}:\\s*([^\\n]+)`, "i");
      const m = text.match(re);
      return m ? m[1].trim() : "";
    };
    entry.assignee = grabLine("Assigned to").replace(/<@([^>]+)>/g, "@$1");
    entry.customerName = grabLine("Name");
    // Email/phone come wrapped in Slack <mailto:|...>  /  <tel:|...> formatting
    const email = grabLine("Email");
    const emailMatch = email.match(/<mailto:([^|>]+)/);
    entry.email = emailMatch ? emailMatch[1] : email;
    const phone = grabLine("Phone");
    const phoneMatch = phone.match(/<tel:([^|>]+)/);
    entry.phone = phoneMatch ? phoneMatch[1] : phone;
    entry.channel = grabLine("Channel");
    entry.page = grabLine("Page");
    entry.keyword = grabLine("Keyword").replace(/^["']|["']$/g, "");
    return entry;
  }

  return entry;
}

// ─── Layer 5: Slack error alerts (Slack conversations.history) ────
// submit-form.js posts a marker-formatted message to #nervo_ops every time
// the catch block fires (unexpected error mid-submission). This pulls those
// messages back so the weekly audit can count them.
async function fetchSlackErrors(fromIso, toIso) {
  const token = process.env.SLACK_BOT_TOKEN;
  const channel = process.env.SLACK_CHANNEL_ID;
  if (!token || !channel) {
    console.warn("SLACK_BOT_TOKEN/SLACK_CHANNEL_ID not set — skipping Slack error count.");
    return { errors: [], accessible: false };
  }

  const oldest = Math.floor(new Date(fromIso).getTime() / 1000).toString();
  const latest = Math.floor(new Date(toIso).getTime() / 1000).toString();
  const all = [];
  let cursor = "";
  let pages = 0;
  while (pages < 20) {  // safety cap: 20 pages × 200 = 4000 messages
    const qs = new URLSearchParams({
      channel, oldest, latest, limit: "200",
      include_all_metadata: "false",
    });
    if (cursor) qs.set("cursor", cursor);
    const r = await fetch(`https://slack.com/api/conversations.history?${qs.toString()}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await r.json();
    if (!data.ok) {
      console.warn(`Slack API error: ${data.error || "unknown"}. Skipping layer.`);
      return { errors: [], accessible: false };
    }
    for (const m of (data.messages || [])) {
      if (m.text && m.text.includes("submit-form ERROR")) {
        all.push({
          ts: m.ts,
          isoTime: new Date(parseFloat(m.ts) * 1000).toISOString(),
          text: m.text,
        });
      }
    }
    if (data.has_more && data.response_metadata && data.response_metadata.next_cursor) {
      cursor = data.response_metadata.next_cursor;
      pages++;
    } else {
      break;
    }
  }
  return { errors: all, accessible: true };
}

// ─── HubSpot contact lookup by phone (cached) ─────────────────────
// Fallback for CallRail calls that don't time-match a HubSpot ticket within
// the ±10min window — the contact exists in HubSpot, just not via a ticket
// association the audit could trace. Searches by last-10-digits (HubSpot's
// CONTAINS_TOKEN matches a normalized phone string).
const _contactByPhoneCache = new Map();
async function getContactByPhone(phone) {
  const norm = normalizePhone(phone);
  if (!norm) return null;
  if (_contactByPhoneCache.has(norm)) return _contactByPhoneCache.get(norm);
  try {
    const r = await hubspot("POST", "/crm/v3/objects/contacts/search", {
      filterGroups: [{
        filters: [{ propertyName: "phone", operator: "CONTAINS_TOKEN", value: norm }],
      }],
      properties: ["phone", "email", "firstname", "lastname"],
      limit: 1,
    });
    const c = r.results && r.results[0] ? r.results[0] : null;
    _contactByPhoneCache.set(norm, c);
    return c;
  } catch {
    _contactByPhoneCache.set(norm, null);
    return null;
  }
}

// ─── Layer 6: Raw submission attempts (#nervo_raw_subs) ───────────
// Reads the structured "submission_attempt {json}" messages submit-form.js
// posts on every exit (since 2026-05-26 deploy). This is the FIRST
// website-direct data layer the audit has — it lets us compute the
// "attempts → contacts → tickets" funnel and detect drops between stages.
async function fetchRawSubmissions(fromIso, toIso) {
  const token = process.env.SLACK_BOT_TOKEN;
  const channel = process.env.SLACK_RAW_CHANNEL_ID;
  if (!token || !channel) {
    console.warn("SLACK_BOT_TOKEN/SLACK_RAW_CHANNEL_ID not set — skipping raw submissions layer.");
    return { attempts: [], accessible: false };
  }
  const oldest = Math.floor(new Date(fromIso).getTime() / 1000).toString();
  const latest = Math.floor(new Date(toIso).getTime() / 1000).toString();
  const all = [];
  let cursor = "";
  let pages = 0;
  while (pages < 20) {
    const qs = new URLSearchParams({ channel, oldest, latest, limit: "200" });
    if (cursor) qs.set("cursor", cursor);
    const r = await fetch(`https://slack.com/api/conversations.history?${qs.toString()}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await r.json();
    if (!data.ok) {
      console.warn(`raw subs Slack API error: ${data.error}`);
      return { attempts: all, accessible: false };
    }
    for (const m of (data.messages || [])) {
      const match = (m.text || "").match(/^submission_attempt\s+(\{.+\})/);
      if (!match) continue;
      try {
        const obj = JSON.parse(match[1]);
        // Slack auto-wraps email in <mailto:|...> when posting via webhook
        if (obj.email) {
          const em = String(obj.email).match(/<mailto:([^|>]+)/);
          if (em) obj.email = em[1];
        }
        obj.ts = m.ts;
        obj.isoTime = new Date(parseFloat(m.ts) * 1000).toISOString();
        all.push(obj);
      } catch { /* malformed JSON — skip */ }
    }
    if (data.has_more && data.response_metadata && data.response_metadata.next_cursor) {
      cursor = data.response_metadata.next_cursor;
      pages++;
    } else {
      break;
    }
  }
  return { attempts: all, accessible: true };
}

// ─── Pipeline classification ───────────────────────────────────────
// Both submit-form.js and CallRail's HubSpot integration create tickets and
// form submissions in the same HubSpot account. We need to bucket each
// record into a pipeline so audits don't conflate them.
function classifyTicket(t) {
  const subj = (t.properties.subject || "").toLowerCase();
  if (subj.startsWith("inbound call")) return "callrail";
  if (subj.startsWith("inbound inquiry") || subj.startsWith("broker inquiry")) return "webform";
  return "other";
}

function classifyFormSub(f) {
  // CallRail's HubSpot integration uses a form literally named "Call Tracking Email"
  if ((f.formName || "").toLowerCase().includes("call tracking")) return "callrail";
  return "webform";
}

// ─── Reconciliation ────────────────────────────────────────────────
function reconcile(contacts, tickets, formSubmissions, callrailCalls) {
  const WINDOW_MS = 60 * 60 * 1000;  // ±60 min match window
  const DUP_WINDOW_MS = 5 * 60 * 1000;

  // Bucket tickets and form submissions by pipeline
  const ticketsByPipeline = { webform: [], callrail: [], other: [] };
  for (const t of tickets) {
    ticketsByPipeline[classifyTicket(t)].push(t);
  }
  const formsByPipeline = { webform: [], callrail: [], other: [] };
  for (const f of formSubmissions) {
    formsByPipeline[classifyFormSub(f)].push(f);
  }

  // Reusable indexers
  const indexTicketsByEmail = (tix) => {
    const m = new Map();
    for (const t of tix) {
      if (!t._email) continue;
      if (!m.has(t._email)) m.set(t._email, []);
      m.get(t._email).push({
        id: t.id,
        createdAt: new Date(t.properties.createdate).getTime(),
        subject: t.properties.subject || "",
        category: t.properties.hs_ticket_category || "",
      });
    }
    return m;
  };
  const indexFormsByEmail = (subs) => {
    const m = new Map();
    for (const f of subs) {
      if (!f.email) continue;
      if (!m.has(f.email)) m.set(f.email, []);
      m.get(f.email).push({
        formId: f.formId,
        formName: f.formName,
        submittedAt: new Date(f.submittedAt).getTime(),
        pageUrl: f.pageUrl,
      });
    }
    return m;
  };

  // ── Webform pipeline reconciliation ──────────────────────────────
  const webform = (() => {
    const ticketsByEmail = indexTicketsByEmail(ticketsByPipeline.webform);
    const formsByEmail = indexFormsByEmail(formsByPipeline.webform);
    const contactsByEmail = new Map();
    for (const c of contacts) {
      const email = (c.properties.email || "").toLowerCase();
      if (!email) continue;
      if (!contactsByEmail.has(email)) contactsByEmail.set(email, []);
      contactsByEmail.get(email).push({
        id: c.id,
        createdAt: new Date(c.properties.createdate).getTime(),
        formSource: c.properties.form_source || "",
      });
    }

    const m = {
      formSubWithoutTicket: [],
      contactWithoutTicket: [],
      duplicateFormSubs: [],
      multipleTickets: [],
      ticketWithoutContact: [],
    };

    // Form sub without ticket (within ±60min)
    for (const [email, subs] of formsByEmail) {
      const tix = ticketsByEmail.get(email) || [];
      for (const sub of subs) {
        const match = tix.find(t => Math.abs(t.createdAt - sub.submittedAt) <= WINDOW_MS);
        if (!match) m.formSubWithoutTicket.push({
          email, formName: sub.formName,
          submittedAt: new Date(sub.submittedAt).toISOString(),
          pageUrl: sub.pageUrl,
        });
      }
    }

    // Contact (form_source set) without ticket
    for (const [email, cs] of contactsByEmail) {
      const tix = ticketsByEmail.get(email) || [];
      for (const c of cs) {
        if (!c.formSource) continue;
        const match = tix.find(t => Math.abs(t.createdAt - c.createdAt) <= WINDOW_MS);
        if (!match) m.contactWithoutTicket.push({
          email, contactId: c.id, formSource: c.formSource,
          createdAt: new Date(c.createdAt).toISOString(),
        });
      }
    }

    // Duplicate form subs within 5min
    for (const [email, subs] of formsByEmail) {
      if (subs.length < 2) continue;
      const sorted = [...subs].sort((a, b) => a.submittedAt - b.submittedAt);
      for (let i = 1; i < sorted.length; i++) {
        if (sorted[i].submittedAt - sorted[i - 1].submittedAt <= DUP_WINDOW_MS) {
          m.duplicateFormSubs.push({
            email,
            firstAt: new Date(sorted[i - 1].submittedAt).toISOString(),
            secondAt: new Date(sorted[i].submittedAt).toISOString(),
            formName: sorted[i].formName,
          });
        }
      }
    }

    // Duplicate tickets within 5min
    for (const [email, tix] of ticketsByEmail) {
      if (tix.length < 2) continue;
      const sorted = [...tix].sort((a, b) => a.createdAt - b.createdAt);
      for (let i = 1; i < sorted.length; i++) {
        if (sorted[i].createdAt - sorted[i - 1].createdAt <= DUP_WINDOW_MS) {
          m.multipleTickets.push({
            email,
            firstTicketId: sorted[i - 1].id,
            secondTicketId: sorted[i].id,
            gapSeconds: Math.round((sorted[i].createdAt - sorted[i - 1].createdAt) / 1000),
          });
        }
      }
    }

    // Tickets without contact assoc
    for (const t of ticketsByPipeline.webform) {
      if (!t._contactId) m.ticketWithoutContact.push({
        ticketId: t.id,
        subject: t.properties.subject,
        createdAt: t.properties.createdate,
      });
    }

    // Breakdowns
    const byCategory = {};
    for (const t of ticketsByPipeline.webform) {
      const k = t.properties.hs_ticket_category || "unknown";
      byCategory[k] = (byCategory[k] || 0) + 1;
    }
    const byFormSource = {};
    for (const c of contacts) {
      const k = c.properties.form_source || "unknown";
      byFormSource[k] = (byFormSource[k] || 0) + 1;
    }

    return {
      counts: {
        contacts: contacts.length,
        tickets: ticketsByPipeline.webform.length,
        formSubmissions: formsByPipeline.webform.length,
      },
      byCategory,
      byFormSource,
      mismatches: m,
    };
  })();

  // ── CallRail pipeline reconciliation ─────────────────────────────
  const callrail = (() => {
    const ticketsByEmail = indexTicketsByEmail(ticketsByPipeline.callrail);
    const formsByEmail = indexFormsByEmail(formsByPipeline.callrail);

    const m = {
      duplicateFormSubs: [],         // same email >1 Call Tracking Email within 5min
      callsWithoutTicket: [],        // CallRail call with no nearby Inbound Call ticket
    };

    // Duplicate "Call Tracking Email" submissions within 5min
    for (const [email, subs] of formsByEmail) {
      if (subs.length < 2) continue;
      const sorted = [...subs].sort((a, b) => a.submittedAt - b.submittedAt);
      for (let i = 1; i < sorted.length; i++) {
        if (sorted[i].submittedAt - sorted[i - 1].submittedAt <= DUP_WINDOW_MS) {
          m.duplicateFormSubs.push({
            email,
            firstAt: new Date(sorted[i - 1].submittedAt).toISOString(),
            secondAt: new Date(sorted[i].submittedAt).toISOString(),
          });
        }
      }
    }

    // Calls without HubSpot ticket — only if CallRail layer is accessible.
    // We match by ±60min on the ticket createdate against the call start_time.
    // (Ticket subjects don't carry the caller email, so we match temporally only.)
    if (callrailCalls && callrailCalls.length) {
      const ticketTimes = ticketsByPipeline.callrail
        .map(t => new Date(t.properties.createdate).getTime())
        .sort((a, b) => a - b);
      for (const call of callrailCalls) {
        if (!call.answered) continue;  // unanswered calls don't usually create tickets
        const callMs = new Date(call.startTime).getTime();
        const match = ticketTimes.find(ts => Math.abs(ts - callMs) <= WINDOW_MS);
        if (!match) m.callsWithoutTicket.push({
          callId: call.id,
          phone: call.customerPhone,
          name: call.customerName,
          city: call.customerCity,
          source: call.source,
          startTime: call.startTime,
        });
      }
    }

    return {
      counts: {
        calls: callrailCalls ? callrailCalls.length : null,
        answeredCalls: callrailCalls ? callrailCalls.filter(c => c.answered).length : null,
        tickets: ticketsByPipeline.callrail.length,
        formSubmissions: formsByPipeline.callrail.length,
      },
      mismatches: m,
    };
  })();

  // ── Unclassified bucket (for visibility) ─────────────────────────
  const other = {
    counts: {
      tickets: ticketsByPipeline.other.length,
      formSubmissions: formsByPipeline.other.length,
    },
    sampleTickets: ticketsByPipeline.other.slice(0, 5).map(t => ({
      ticketId: t.id,
      subject: t.properties.subject,
    })),
    sampleForms: formsByPipeline.other.slice(0, 5).map(f => ({
      formName: f.formName,
      email: f.email,
    })),
  };

  return { webform, callrail, other };
}

// ─── Slack formatting ──────────────────────────────────────────────
// ─── Aggregate raw submission outcomes by category ────────────────
function aggregateAttempts(attempts) {
  const buckets = {
    success: 0,           // success + success_broker
    honeypot: 0,
    turnstile: 0,         // missing + failed
    rate_limit: 0,        // rate_limited + daily_cap
    timestamp: 0,
    validation: 0,        // all validation_* outcomes
    disposable: 0,
    error: 0,
    unknown: 0,
  };
  for (const a of attempts) {
    const o = a.outcome || "";
    if (o === "success" || o === "success_broker") buckets.success++;
    else if (o === "honeypot_blocked") buckets.honeypot++;
    else if (o === "turnstile_missing" || o === "turnstile_failed") buckets.turnstile++;
    else if (o === "rate_limited" || o === "daily_cap_exceeded") buckets.rate_limit++;
    else if (o === "timestamp_too_fast") buckets.timestamp++;
    else if (o && o.startsWith("validation_")) buckets.validation++;
    else if (o === "disposable_email_blocked") buckets.disposable++;
    else if (o === "error") buckets.error++;
    else buckets.unknown++;
  }
  return buckets;
}

// ─── Aggregate calls by salesperson (call-pipeline allocation) ────
function aggregateCallsByPerson(callrailCalls) {
  const m = new Map();  // name → { answered, voicemail, missed }
  for (const c of callrailCalls) {
    const name = phoneToName(c.businessPhone) || "(unmapped)";
    if (!m.has(name)) m.set(name, { name, answered: 0, voicemail: 0, missed: 0 });
    const b = m.get(name);
    if (c.answered) b.answered++;
    else if (c.voicemail) b.voicemail++;
    else b.missed++;
  }
  return [...m.values()].sort((a, b) => (b.answered + b.voicemail + b.missed) - (a.answered + a.voicemail + a.missed));
}

// ─── Aggregate web contacts by owner (web-pipeline allocation) ────
function aggregateWebContactsByOwner(contacts, ownersById) {
  const m = new Map();  // name → count
  for (const c of contacts) {
    const ownerId = c.properties.hubspot_owner_id || "";
    let name;
    if (!ownerId) name = "(unassigned)";
    else if (ownersById.has(String(ownerId))) name = ownersById.get(String(ownerId)).fullName;
    else name = `Owner ${ownerId}`;
    m.set(name, (m.get(name) || 0) + 1);
  }
  return [...m.entries()]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => {
      const aUn = a.name.startsWith("(unassigned)") || a.name.startsWith("Owner ");
      const bUn = b.name.startsWith("(unassigned)") || b.name.startsWith("Owner ");
      if (aUn !== bUn) return aUn ? 1 : -1;
      return b.count - a.count;
    });
}

// ─── Detect call voicemail status (need to fetch the field) ───────
function callStatusCounts(callrailCalls) {
  let answered = 0, voicemail = 0, missed = 0;
  for (const c of callrailCalls) {
    if (c.answered) answered++;
    else if (c.voicemail) voicemail++;
    else missed++;
  }
  return { answered, voicemail, missed };
}

// ─── Severity-tiered issues for the one-pager ─────────────────────
// Each item carries an optional contactId/ticketId so the renderer can produce
// clickable HubSpot links instead of bare emails.
function buildSeverityIssues(report, callrailCalls, tickets, contactByEmail) {
  const tiers = { critical: [], warning: [], info: [] };
  const m = report.webform.mismatches;
  const cm = report.callrail.mismatches;

  // Helper: find contact id from email via the fetched contacts list
  const cidByEmail = (email) => {
    if (!email) return null;
    const c = contactByEmail.get(email.toLowerCase());
    return c ? c.id : null;
  };
  // Helper: find contact id from a ticket id
  const cidByTicket = (ticketId) => {
    const t = tickets.find(x => x.id === String(ticketId));
    return t ? t._contactId : null;
  };

  // CRITICAL — actual lost / missing data
  for (const x of m.contactWithoutTicket) tiers.critical.push({ type: "Contact with form_source but no ticket", email: x.email, contactId: x.contactId });
  if (report.functionErrors && report.functionErrors.errors.length) {
    tiers.critical.push({ type: `${report.functionErrors.errors.length} submit-form function error(s)`, detail: "see #nervo_ops scrollback" });
  }
  for (const x of cm.callsWithoutTicket) tiers.critical.push({ type: "Answered call with no HubSpot ticket", phone: x.phone, detail: x.startTime });

  // WARNING — drift, duplicates, cleanup
  for (const x of m.multipleTickets) tiers.warning.push({
    type: "Duplicate webform tickets (5min)",
    email: x.email,
    contactId: cidByEmail(x.email) || cidByTicket(x.firstTicketId),
    detail: `gap ${x.gapSeconds}s`,
  });
  for (const x of m.duplicateFormSubs) tiers.warning.push({ type: "Duplicate webform form-subs (5min)", email: x.email, contactId: cidByEmail(x.email) });
  for (const x of cm.duplicateFormSubs) tiers.warning.push({ type: "Duplicate Call Tracking Email (5min)", email: x.email, contactId: cidByEmail(x.email) });
  for (const x of m.ticketWithoutContact) tiers.warning.push({ type: "Webform ticket without contact assoc", ticketId: x.ticketId });

  // INFO — attribution gaps, dual-capture noise
  for (const x of m.formSubWithoutTicket) tiers.info.push({ type: "HS form-sub w/ no matching ticket", email: x.email, contactId: cidByEmail(x.email), detail: x.formName });

  return tiers;
}

// ─── NEW: Concise one-pager Slack format ──────────────────────────
function buildOnePagerSlackPayload(report, args, contacts, tickets, callrailCalls, rawSubmissionsResult, ownersById) {
  const sections = [];
  const dim = (n, total) => total > 0 ? ` (${Math.round((n / total) * 100)}%)` : "";
  const portal = process.env.HUBSPOT_PORTAL_ID;
  const contactUrl = (id) => portal && id ? `https://app.hubspot.com/contacts/${portal}/contact/${id}` : "";
  const ticketUrl = (id) => portal && id ? `https://app.hubspot.com/contacts/${portal}/ticket/${id}` : "";
  const link = (text, url) => url ? `<${url}|${text}>` : text;

  // ── Build lookup maps for contact resolution ─────────────────
  // Email → contact (from our fetched contacts list)
  const contactByEmail = new Map();
  // Phone → contact (normalized; for cross-channel matching)
  const contactByPhone = new Map();
  for (const c of contacts) {
    const p = c.properties || {};
    if (p.email) contactByEmail.set(p.email.toLowerCase(), c);
    if (p.phone) {
      const norm = normalizePhone(p.phone);
      if (norm) contactByPhone.set(norm, c);
    }
  }
  // For tickets we also have _contactId — useful for callrail calls (their HubSpot
  // contact may have been created by the integration without form_source set, so
  // it's not in our `contacts` array). Build a phone → contactId fallback.
  const phoneToCallContactId = new Map();
  for (const t of tickets) {
    if (classifyTicket(t) !== "callrail") continue;
    if (t._contactId) {
      // We don't know the contact's phone from the ticket alone — leave it for
      // the per-call match below to fill via the matched-ticket pathway.
    }
  }
  // Match each CallRail call to its closest CallRail ticket (same logic as the
  // Reconciliation tab), then to that ticket's associated contact.
  const sortedCallrailTickets = tickets
    .filter(t => classifyTicket(t) === "callrail")
    .map(t => ({ t, ms: new Date(t.properties.createdate).getTime() }))
    .sort((a, b) => a.ms - b.ms);
  const callToContactId = (call) => {
    const callMs = new Date(call.startTime).getTime();
    let best = null;
    for (const { t, ms } of sortedCallrailTickets) {
      const gap = Math.abs(ms - callMs);
      if (gap <= 10 * 60 * 1000 && (!best || gap < best.gap)) best = { t, gap };
    }
    return best && best.t._contactId ? best.t._contactId : null;
  };

  // Header
  sections.push(`*Weekly Audit* — ${args.from} → ${args.to}`);
  sections.push("");

  // ── Web Leads ────────────────────────────────────────────────
  const att = rawSubmissionsResult.accessible ? aggregateAttempts(rawSubmissionsResult.attempts) : null;
  const totalAttempts = att ? Object.values(att).reduce((a, b) => a + b, 0) : null;
  const contactsCount = report.webform.counts.contacts;
  const ticketsCount = report.webform.counts.tickets;

  // Detect whether the raw logging covered the full audit window. The per-attempt
  // logger went live 2026-05-26 — if args.from is earlier, the funnel headline
  // would be misleading (attempts-count is for partial window, contacts for full).
  const ragsStart = rawSubmissionsResult.attempts.length > 0
    ? new Date(Math.min(...rawSubmissionsResult.attempts.map(a => parseFloat(a.ts) * 1000)))
    : null;
  const fromMs = new Date(args.from + "T00:00:00Z").getTime();
  const rawCoversFullWindow = ragsStart && (ragsStart.getTime() - fromMs) < 24 * 60 * 60 * 1000;

  // Compute multi-submit emails to explain the contacts-vs-tickets gap
  const webformTicketsByEmail = new Map();
  const webformTickets = tickets.filter(t => classifyTicket(t) === "webform");
  for (const t of webformTickets) {
    const e = (t._email || "").toLowerCase();
    if (!e) continue;
    if (!webformTicketsByEmail.has(e)) webformTicketsByEmail.set(e, 0);
    webformTicketsByEmail.set(e, webformTicketsByEmail.get(e) + 1);
  }
  const uniqueEmails = webformTicketsByEmail.size;
  const multiSubmitEmails = [...webformTicketsByEmail.values()].filter(c => c > 1).length;
  const extraTickets = ticketsCount - uniqueEmails;  // tickets above unique-emails baseline

  sections.push(":globe_with_meridians: *Web Leads*");
  if (att && rawCoversFullWindow && totalAttempts > 0) {
    sections.push(`  *${totalAttempts}* attempts → ${att.success} succeeded → ${contactsCount} contacts → ${ticketsCount} tickets`);
    const parts = [];
    if (att.honeypot) parts.push(`${att.honeypot} honeypot`);
    if (att.turnstile) parts.push(`${att.turnstile} turnstile`);
    if (att.rate_limit) parts.push(`${att.rate_limit} rate-limit`);
    if (att.timestamp) parts.push(`${att.timestamp} timestamp`);
    if (att.validation) parts.push(`${att.validation} validation`);
    if (att.disposable) parts.push(`${att.disposable} disposable`);
    if (parts.length) sections.push(`    _rejected as designed: ${parts.join(", ")}_`);
    if (att.error > 0) sections.push(`    :warning: *${att.error} function error${att.error === 1 ? "" : "s"}* — see #nervo_ops`);
    if (att.success !== contactsCount) {
      sections.push(`    :warning: drop between successful exits (${att.success}) and HubSpot contacts (${contactsCount}) — possible data loss`);
    }
  } else {
    sections.push(`  *${contactsCount}* contacts created → ${ticketsCount} tickets`);
    if (extraTickets > 0) {
      const m = report.webform.mismatches.multipleTickets.length;
      sections.push(`  _${extraTickets} extra ticket${extraTickets === 1 ? "" : "s"} = same-email resubmits (${m} within 5min flagged as drift; ${multiSubmitEmails - m} spaced out, treated as legit returning users)_`);
    }
    if (att && totalAttempts > 0) {
      const startStr = ragsStart.toISOString().slice(0, 10);
      sections.push(`  _per-attempt logging started ${startStr} — full funnel available next week_`);
    } else if (!rawSubmissionsResult.accessible) {
      sections.push(`  _per-attempt logging not configured_`);
    }
  }
  sections.push("");
  sections.push("  *Contact-owner allocation:*");
  const webByPerson = aggregateWebContactsByOwner(contacts, ownersById);
  const webLines = webByPerson.map(p => `    ${p.name.padEnd(22)} ${String(p.count).padStart(3)}${dim(p.count, contactsCount)}`);
  sections.push("```\n" + webLines.join("\n") + "\n```");

  // Single-line drill-in for unassigned bucket
  const unassignedContacts = contacts.filter(c => !(c.properties && c.properties.hubspot_owner_id));
  if (unassignedContacts.length) {
    const items = unassignedContacts.map(c => {
      const p = c.properties || {};
      const name = [p.firstname, p.lastname].filter(Boolean).join(" ") || p.email || `#${c.id}`;
      const tag = p.form_source || "?";
      return `${link(name, contactUrl(c.id))} (${tag})`;
    });
    sections.push(`  *Unassigned breakdown:* ${items.join(", ")}`);
  }
  sections.push("");

  // ── Calls ────────────────────────────────────────────────────
  const cs = callStatusCounts(callrailCalls);
  const totalCalls = cs.answered + cs.voicemail + cs.missed;
  sections.push(":telephone_receiver: *Calls*");
  sections.push(`  ${totalCalls} received — *${cs.answered} answered*, ${cs.voicemail} voicemail, ${cs.missed} missed`);
  sections.push(`  HubSpot \"Inbound Call\" tickets: ${report.callrail.counts.tickets}`);
  sections.push("");

  // Per-person bullets — group by forwarded-to person, then list VM / Missed
  // names inline with contact links (uses call._resolvedContactId pre-computed
  // in main() so we get a link even when no CallRail ticket exists yet).
  const byPerson = new Map();  // person → { answered, vmCalls, missedCalls }
  for (const c of callrailCalls) {
    const name = phoneToName(c.businessPhone) || "(unmapped)";
    if (!byPerson.has(name)) byPerson.set(name, { name, answered: 0, vmCalls: [], missedCalls: [] });
    const b = byPerson.get(name);
    if (c.answered) b.answered++;
    else if (c.voicemail) b.vmCalls.push(c);
    else b.missedCalls.push(c);
  }
  // Sort by total volume, unmapped last
  const personEntries = [...byPerson.values()].sort((a, b) => {
    const aUn = a.name === "(unmapped)", bUn = b.name === "(unmapped)";
    if (aUn !== bUn) return aUn ? 1 : -1;
    return (b.answered + b.vmCalls.length + b.missedCalls.length) - (a.answered + a.vmCalls.length + a.missedCalls.length);
  });
  const renderCallName = (c) => {
    const display = c.customerName || c.customerPhone || "?";
    return c._resolvedContactId ? link(display, contactUrl(c._resolvedContactId)) : display;
  };
  for (const p of personEntries) {
    const total = p.answered + p.vmCalls.length + p.missedCalls.length;
    sections.push(`  • *${p.name}* — ${p.answered} answered, ${p.vmCalls.length} vm, ${p.missedCalls.length} missed (total ${total})`);
    if (p.vmCalls.length) {
      sections.push(`      VM: ${p.vmCalls.map(renderCallName).join(", ")}`);
    }
    if (p.missedCalls.length) {
      sections.push(`      Missed: ${p.missedCalls.map(renderCallName).join(", ")}`);
    }
  }
  sections.push("");

  // ── Cross-channel leads (form + call) — placed after Calls ────
  const crossChannel = [];
  for (const c of contacts) {
    const p = c.properties || {};
    if (!p.phone) continue;
    const norm = normalizePhone(p.phone);
    if (!norm) continue;
    const matchingCall = callrailCalls.find(call => normalizePhone(call.customerPhone) === norm);
    if (matchingCall) crossChannel.push({ contact: c, call: matchingCall });
  }
  if (crossChannel.length) {
    sections.push(`:star: *Cross-channel leads* (${crossChannel.length}) — submitted a form *and* called:`);
    for (const { contact, call } of crossChannel) {
      const p = contact.properties || {};
      const name = [p.firstname, p.lastname].filter(Boolean).join(" ") || p.email || `#${contact.id}`;
      const callDay = call.startTime ? call.startTime.slice(0, 10) : "?";
      const callerLine = phoneToName(call.businessPhone) || call.businessPhone || "?";
      const status = call.answered ? "" : (call.voicemail ? " [vm]" : " [missed]");
      sections.push(`  • ${link(name, contactUrl(contact.id))} — ${p.form_source || "?"} + call ${callDay} → ${callerLine}${status}`);
    }
    sections.push("");
  }

  // ── Issues ───────────────────────────────────────────────────
  const tiers = buildSeverityIssues(report, callrailCalls, tickets, contactByEmail);
  const totalIssues = tiers.critical.length + tiers.warning.length + tiers.info.length;
  sections.push(totalIssues === 0
    ? ":white_check_mark: *No issues this week*"
    : `:warning: *Issues this week* — ${totalIssues}`);

  const renderItem = (i) => {
    // Build the contact reference: prefer link if we have contactId, else bare email/phone
    let ref = "";
    if (i.contactId) ref = i.email ? link(i.email, contactUrl(i.contactId)) : link("contact", contactUrl(i.contactId));
    else if (i.email) ref = i.email;
    else if (i.phone) ref = i.phone;
    else if (i.ticketId) ref = link(`#${i.ticketId}`, ticketUrl(i.ticketId));
    const parts = [i.type];
    if (ref) parts.push(ref);
    if (i.detail) parts.push(`(${i.detail})`);
    return `    • ${parts.join(" — ")}`;
  };
  const renderTier = (label, items) => {
    if (!items.length) return null;
    const lines = [`  ${label} (${items.length}):`];
    items.forEach(i => lines.push(renderItem(i)));
    return lines.join("\n");
  };
  const cr = renderTier(":red_circle: Critical (lost data)", tiers.critical);
  const wr = renderTier(":large_yellow_circle: Drift / cleanup", tiers.warning);
  const ir = renderTier(":white_circle: Informational", tiers.info);
  if (cr) sections.push(cr);
  if (wr) sections.push(wr);
  if (ir) sections.push(ir);

  return { text: sections.join("\n") };
}

function buildSlackPayload(report, args, formsLayerOk, callrailLayerOk) {
  const portal = process.env.HUBSPOT_PORTAL_ID;
  const contactUrl = (id) => portal ? `https://app.hubspot.com/contacts/${portal}/contact/${id}` : id;
  const ticketUrl = (id) => portal ? `https://app.hubspot.com/contacts/${portal}/ticket/${id}` : id;

  const w = report.webform.mismatches;
  const c = report.callrail.mismatches;
  const totalIssues =
    w.formSubWithoutTicket.length + w.contactWithoutTicket.length +
    w.duplicateFormSubs.length + w.multipleTickets.length + w.ticketWithoutContact.length +
    c.duplicateFormSubs.length + c.callsWithoutTicket.length;

  const fmtCounts = (obj) => Object.entries(obj)
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => `  • \`${k}\`: ${v}`)
    .join("\n") || "  _(none)_";

  const fmtList = (arr, render, cap = 15) => {
    if (!arr.length) return "_None_ ✅";
    const shown = arr.slice(0, cap).map(render).join("\n");
    const extra = arr.length > cap ? `\n_…and ${arr.length - cap} more_` : "";
    return shown + extra;
  };

  const header = totalIssues === 0
    ? `:white_check_mark: *Submission Audit* — ${args.from} → ${args.to}`
    : `:warning: *Submission Audit* — ${args.from} → ${args.to} — *${totalIssues} issue${totalIssues === 1 ? "" : "s"}*`;

  const sections = [header, ""];

  // ── Webform pipeline ──────────────────────────────────────────
  sections.push("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  sections.push(":globe_with_meridians: *Webform pipeline* (ledgertc.com → `submit-form.js` → HubSpot)");
  sections.push("");
  sections.push("*Counts*");
  sections.push(`  • Contacts with form_source set: *${report.webform.counts.contacts}*`);
  sections.push(`  • Tickets created: *${report.webform.counts.tickets}*`);
  sections.push(`  • HubSpot form submissions: *${report.webform.counts.formSubmissions}*` + (formsLayerOk ? "" : " _(forms layer unavailable)_"));
  sections.push("");
  sections.push("*Tickets by category*");
  sections.push(fmtCounts(report.webform.byCategory));
  sections.push("");
  sections.push("*Contacts by form_source*");
  sections.push(fmtCounts(report.webform.byFormSource));

  // submit-form.js error count (from Slack history)
  if (report.functionErrors) {
    sections.push("");
    sections.push("*submit-form function errors*");
    if (report.functionErrors.accessible) {
      const n = report.functionErrors.errors.length;
      if (n === 0) {
        sections.push("  • _None in window_ ✅");
      } else {
        sections.push(`  • *${n}* unexpected error${n === 1 ? "" : "s"} caught (real-time alerts above in channel scrollback)`);
        // Show the first few error timestamps for quick reference
        const shown = report.functionErrors.errors.slice(0, 5);
        for (const e of shown) {
          sections.push(`    – ${e.isoTime}: ${e.text.replace(/^:rotating_light:\s*\*?submit-form ERROR\*?\s*—\s*/i, "").slice(0, 140)}`);
        }
        if (report.functionErrors.errors.length > 5) {
          sections.push(`    – _…and ${report.functionErrors.errors.length - 5} more_`);
        }
      }
    } else {
      sections.push("  • _(Slack history unavailable — set SLACK_BOT_TOKEN + SLACK_CHANNEL_ID)_");
    }
  }

  if (w.formSubWithoutTicket.length) {
    sections.push("", `*Form sub with no matching ticket* (${w.formSubWithoutTicket.length}) — dual-capture or function failure`);
    sections.push(fmtList(w.formSubWithoutTicket, x =>
      `  • ${x.email} — _${x.formName}_ @ ${x.submittedAt}${x.pageUrl ? ` (${x.pageUrl})` : ""}`
    ));
  }
  if (w.contactWithoutTicket.length) {
    sections.push("", `*Contact with form_source but no ticket* (${w.contactWithoutTicket.length}) — function may have crashed mid-flow`);
    sections.push(fmtList(w.contactWithoutTicket, x =>
      `  • ${x.email} — _${x.formSource}_ @ ${x.createdAt} — <${contactUrl(x.contactId)}|contact>`
    ));
  }
  if (w.duplicateFormSubs.length) {
    sections.push("", `*Duplicate form submissions* within 5min (${w.duplicateFormSubs.length}) — dual-capture bug`);
    sections.push(fmtList(w.duplicateFormSubs, x =>
      `  • ${x.email} — _${x.formName}_ — ${x.firstAt} & ${x.secondAt}`
    ));
  }
  if (w.multipleTickets.length) {
    sections.push("", `*Duplicate tickets* within 5min (${w.multipleTickets.length}) — user resubmitted or retry storm`);
    sections.push(fmtList(w.multipleTickets, x =>
      `  • ${x.email} — gap ${x.gapSeconds}s — <${ticketUrl(x.firstTicketId)}|#${x.firstTicketId}>, <${ticketUrl(x.secondTicketId)}|#${x.secondTicketId}>`
    ));
  }
  if (w.ticketWithoutContact.length) {
    sections.push("", `*Tickets with no associated contact* (${w.ticketWithoutContact.length}) — data integrity`);
    sections.push(fmtList(w.ticketWithoutContact, x =>
      `  • <${ticketUrl(x.ticketId)}|#${x.ticketId}> — ${x.subject} — ${x.createdAt}`
    ));
  }

  // ── CallRail pipeline ─────────────────────────────────────────
  sections.push("");
  sections.push("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  sections.push(":telephone_receiver: *CallRail pipeline* (phone calls → CallRail → HubSpot)");
  sections.push("");
  sections.push("*Counts*");
  if (callrailLayerOk && report.callrail.counts.calls !== null) {
    sections.push(`  • CallRail calls (total): *${report.callrail.counts.calls}*`);
    sections.push(`  • CallRail calls (answered): *${report.callrail.counts.answeredCalls}*`);
  } else {
    sections.push(`  • CallRail calls: _(CallRail API not configured — set CALLRAIL_API_KEY + CALLRAIL_ACCOUNT_ID)_`);
  }
  sections.push(`  • HubSpot "Inbound Call" tickets: *${report.callrail.counts.tickets}*`);
  sections.push(`  • HubSpot "Call Tracking Email" form submissions: *${report.callrail.counts.formSubmissions}*`);

  if (c.callsWithoutTicket.length) {
    sections.push("", `*Answered CallRail calls with no nearby ticket* (${c.callsWithoutTicket.length}) — call received but HubSpot ticket missing`);
    sections.push(fmtList(c.callsWithoutTicket, x =>
      `  • ${x.phone}${x.name ? ` (${x.name})` : ""}${x.city ? `, ${x.city}` : ""} — _${x.source || "no source"}_ @ ${x.startTime}`
    ));
  }
  if (c.duplicateFormSubs.length) {
    sections.push("", `*Duplicate "Call Tracking Email" submissions* within 5min (${c.duplicateFormSubs.length})`);
    sections.push(fmtList(c.duplicateFormSubs, x =>
      `  • ${x.email} — ${x.firstAt} & ${x.secondAt}`
    ));
  }

  // ── Unclassified (low-priority surface) ───────────────────────
  if (report.other.counts.tickets > 0 || report.other.counts.formSubmissions > 0) {
    sections.push("");
    sections.push("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    sections.push(":question: *Unclassified*");
    sections.push(`  • Tickets: *${report.other.counts.tickets}*`);
    sections.push(`  • Form submissions: *${report.other.counts.formSubmissions}*`);
    if (report.other.sampleTickets.length) {
      sections.push("  _Sample ticket subjects:_ " + report.other.sampleTickets.map(t => `"${t.subject}"`).join(", "));
    }
    if (report.other.sampleForms.length) {
      sections.push("  _Sample form names:_ " + report.other.sampleForms.map(f => `"${f.formName}"`).join(", "));
    }
  }

  return { text: sections.join("\n") };
}

async function postSlack(payload, channelId) {
  // If channelId is provided, use chat.postMessage with the bot token (lets us
  // target arbitrary channels the bot is a member of). Otherwise use the
  // configured incoming webhook (fixed to #nervo_ops).
  if (channelId) {
    const token = process.env.SLACK_BOT_TOKEN;
    if (!token) throw new Error("SLACK_BOT_TOKEN env var required for --slack-channel posts");
    const res = await fetch("https://slack.com/api/chat.postMessage", {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ channel: channelId, text: payload.text, unfurl_links: false, unfurl_media: false }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(`Slack chat.postMessage failed: ${data.error}`);
    return;
  }
  const url = process.env.SLACK_WEBHOOK_URL;
  if (!url) throw new Error("SLACK_WEBHOOK_URL env var is required (or pass --dry-run)");
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Slack webhook ${res.status}: ${body}`);
  }
}

// ─── Salesperson allocation ───────────────────────────────────────
// Aggregates tickets/contacts/calls by salesperson. Uses email as the join
// key so HubSpot owners and CallRail agents collapse into one row when
// they're the same person.
function buildSalespersonAllocation(contacts, tickets, callrailCalls, ownersById) {
  // key = lowercased email (or `unassigned` / `unknown:<id>` if no email)
  const agg = new Map();
  const getOrInit = (key, displayName) => {
    if (!agg.has(key)) {
      agg.set(key, {
        key,
        email: key.startsWith("unassigned") || key.startsWith("unknown:") ? "" : key,
        name: displayName || key,
        webformTickets: 0,
        callrailTickets: 0,
        contactsOwned: 0,
        callsAnswered: 0,
      });
    }
    return agg.get(key);
  };

  const resolveOwner = (ownerId) => {
    if (!ownerId) return { key: "unassigned", name: "(unassigned)" };
    const o = ownersById.get(String(ownerId));
    if (!o) return { key: `unknown:${ownerId}`, name: `Owner ${ownerId}` };
    return { key: o.email || `unknown:${ownerId}`, name: o.fullName };
  };

  // Tickets, classified
  for (const t of tickets) {
    const { key, name } = resolveOwner(t.properties.hubspot_owner_id);
    const bucket = getOrInit(key, name);
    if (classifyTicket(t) === "webform") bucket.webformTickets++;
    else if (classifyTicket(t) === "callrail") bucket.callrailTickets++;
  }

  // Contacts (webform — only ones with form_source set)
  for (const c of contacts) {
    const { key, name } = resolveOwner(c.properties.hubspot_owner_id);
    const bucket = getOrInit(key, name);
    bucket.contactsOwned++;
  }

  // CallRail calls answered — map business_phone_number (forwarded-to) →
  // salesperson name via the hardcoded CALLRAIL_PHONE_TO_NAME table (since
  // agent_email is empty for all calls). When the same person is also a
  // HubSpot owner (matched by first name), buckets merge.
  for (const call of callrailCalls) {
    if (!call.answered) continue;
    const mappedName = phoneToName(call.businessPhone);
    if (!mappedName) {
      const key = "unmapped-phone:" + (normalizePhone(call.businessPhone) || "none");
      const bucket = getOrInit(key, `(unmapped: ${call.businessPhone || "no #"})`);
      bucket.callsAnswered++;
      continue;
    }
    // Find this person in HubSpot owners by first name (or full name match)
    let displayName = mappedName;
    let key = mappedName.toLowerCase();
    for (const o of ownersById.values()) {
      if ((o.firstName && o.firstName.toLowerCase() === mappedName.toLowerCase()) ||
          (o.fullName && o.fullName.toLowerCase().startsWith(mappedName.toLowerCase() + " "))) {
        displayName = o.fullName;
        key = o.email || key;
        break;
      }
    }
    const bucket = getOrInit(key, displayName);
    bucket.callsAnswered++;
  }

  // Compute total tickets and percentages
  const rows = [...agg.values()].map(r => ({
    ...r,
    totalTickets: r.webformTickets + r.callrailTickets,
  }));
  const grandTotal = rows.reduce((sum, r) => sum + r.totalTickets, 0);
  for (const r of rows) {
    r.pctOfTickets = grandTotal > 0 ? (r.totalTickets / grandTotal) : 0;
  }

  // Sort by total tickets desc (unassigned bucket sinks to bottom regardless)
  rows.sort((a, b) => {
    const aUnassigned = a.key.startsWith("unassigned") || a.key.startsWith("unknown:");
    const bUnassigned = b.key.startsWith("unassigned") || b.key.startsWith("unknown:");
    if (aUnassigned !== bUnassigned) return aUnassigned ? 1 : -1;
    return b.totalTickets - a.totalTickets;
  });

  return { rows, grandTotal };
}

// ─── XLSX export ──────────────────────────────────────────────────
// Generates a multi-tab Excel workbook with the raw rows behind the
// summary stats, plus clickable HubSpot links. Open in Google Sheets
// by uploading to Drive (Sheets auto-imports XLSX preserving tabs +
// hyperlinks).
// ─── Lightweight 2-tab XLSX (Allocation + Issues) ─────────────────
// Drill-down companion to the Slack one-pager. Per-person allocation grid +
// severity-tiered issue list with HubSpot links.
async function buildLightweightXlsx(args, contacts, tickets, callrailCalls, report, ownersById, outputPath) {
  const ExcelJS = require("exceljs");
  const portal = process.env.HUBSPOT_PORTAL_ID;
  const contactUrl = (id) => portal && id ? `https://app.hubspot.com/contacts/${portal}/contact/${id}` : "";
  const ticketUrl = (id) => portal && id ? `https://app.hubspot.com/contacts/${portal}/ticket/${id}` : "";

  const LEDGER_GREEN = "FF2B4A42";
  const LEDGER_GOLD = "FFC49A3C";
  const LEDGER_CREAM = "FFF2EFE9";
  const TIER_CRITICAL = "FFFFC9C9";
  const TIER_WARNING = "FFFFE699";
  const LINK_BLUE = "FF0563C1";

  const wb = new ExcelJS.Workbook();
  wb.creator = "audit-submissions.js";
  wb.created = new Date();

  const styleLink = (cell) => {
    if (cell.value && typeof cell.value === "object" && cell.value.hyperlink) {
      cell.font = { color: { argb: LINK_BLUE }, underline: true };
    }
  };

  // Helper: create a sheet with title + source rows on top
  function newSheet(name, title, source, columns) {
    const ws = wb.addWorksheet(name);
    ws.columns = columns.map(c => ({ ...c, header: undefined }));
    const colCount = columns.length;
    const lastCol = String.fromCharCode(64 + colCount);

    ws.mergeCells(`A1:${lastCol}1`);
    const t = ws.getCell("A1");
    t.value = title;
    t.font = { size: 14, bold: true, color: { argb: "FFFFFFFF" } };
    t.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GREEN } };
    t.alignment = { vertical: "middle", indent: 1 };
    ws.getRow(1).height = 24;

    ws.mergeCells(`A2:${lastCol}2`);
    const s = ws.getCell("A2");
    s.value = `Source: ${source}`;
    s.font = { size: 10, italic: true, color: { argb: LEDGER_GREEN } };
    s.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GOLD } };
    s.alignment = { vertical: "middle", indent: 1 };
    ws.getRow(2).height = 18;

    ws.getRow(3).height = 6;

    const headerRow = ws.getRow(4);
    columns.forEach((c, i) => { headerRow.getCell(i + 1).value = c.header; });
    headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
    headerRow.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GREEN } };
    headerRow.alignment = { vertical: "middle" };
    headerRow.height = 18;

    ws.views = [{ state: "frozen", ySplit: 4 }];
    return ws;
  }

  // ── Tab: Allocation ───────────────────────────────────────────
  const allocWs = newSheet(
    "Allocation",
    "Allocation — per-salesperson breakdown",
    "Web contacts grouped by HubSpot contact owner; calls grouped by CallRail forwarded-to number → name map.",
    [
      { key: "name", header: "Salesperson", width: 26 },
      { key: "webContacts", header: "Web Contacts", width: 14 },
      { key: "callsAnswered", header: "Calls Answered", width: 16 },
      { key: "callsVoicemail", header: "Voicemail", width: 12 },
      { key: "callsMissed", header: "Missed", width: 10 },
      { key: "total", header: "Total Allocations", width: 18 },
      { key: "pct", header: "% of Total", width: 12 },
    ]
  );

  // Aggregate
  const webByOwner = aggregateWebContactsByOwner(contacts, ownersById);
  const callsByPerson = aggregateCallsByPerson(callrailCalls);
  const merged = new Map();  // name → { webContacts, callsAnswered, callsVoicemail, callsMissed }
  const getOrInit = (name) => {
    if (!merged.has(name)) merged.set(name, { name, webContacts: 0, callsAnswered: 0, callsVoicemail: 0, callsMissed: 0 });
    return merged.get(name);
  };
  for (const { name, count } of webByOwner) getOrInit(name).webContacts = count;
  // Merge call entries by matching the first-name part of HubSpot owner names
  for (const cp of callsByPerson) {
    // Try to merge into a HubSpot owner row whose first-name matches the phone-mapped name
    let mergeKey = null;
    for (const k of merged.keys()) {
      if (k.split(/\s+/)[0].toLowerCase() === cp.name.toLowerCase()) { mergeKey = k; break; }
    }
    const bucket = getOrInit(mergeKey || cp.name);
    bucket.callsAnswered += cp.answered;
    bucket.callsVoicemail += cp.voicemail;
    bucket.callsMissed += cp.missed;
  }
  const rows = [...merged.values()].map(r => ({
    ...r,
    total: r.webContacts + r.callsAnswered + r.callsVoicemail + r.callsMissed,
  }));
  const grandTotal = rows.reduce((s, r) => s + r.total, 0);
  for (const r of rows) r.pct = grandTotal > 0 ? r.total / grandTotal : 0;
  rows.sort((a, b) => {
    const aUn = a.name.startsWith("(unassigned)") || a.name.startsWith("(unmapped)") || a.name.startsWith("Owner ");
    const bUn = b.name.startsWith("(unassigned)") || b.name.startsWith("(unmapped)") || b.name.startsWith("Owner ");
    if (aUn !== bUn) return aUn ? 1 : -1;
    return b.total - a.total;
  });

  for (const r of rows) {
    const row = allocWs.addRow(r);
    row.getCell("pct").numFmt = "0.0%";
    if (r.name.startsWith("(unassigned)") || r.name.startsWith("(unmapped)") || r.name.startsWith("Owner ")) {
      for (let i = 1; i <= 7; i++) {
        row.getCell(i).fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
        row.getCell(i).font = { italic: true };
      }
    }
  }
  // Total row
  const totalRow = allocWs.addRow({
    name: "Total",
    webContacts: rows.reduce((s, r) => s + r.webContacts, 0),
    callsAnswered: rows.reduce((s, r) => s + r.callsAnswered, 0),
    callsVoicemail: rows.reduce((s, r) => s + r.callsVoicemail, 0),
    callsMissed: rows.reduce((s, r) => s + r.callsMissed, 0),
    total: grandTotal,
    pct: 1,
  });
  totalRow.font = { bold: true };
  totalRow.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GREEN } };
  for (let i = 1; i <= 7; i++) totalRow.getCell(i).font = { bold: true, color: { argb: "FFFFFFFF" } };
  totalRow.getCell("pct").numFmt = "0.0%";

  // ── Tab: Issues ───────────────────────────────────────────────
  const issuesWs = newSheet(
    "Issues",
    "Issues — severity-tiered",
    "Critical = lost/missing data. Warning = drift, duplicates, cleanup. Info = attribution gaps, dual-capture noise.",
    [
      { key: "tier", header: "Severity", width: 14 },
      { key: "type", header: "Type", width: 38 },
      { key: "email", header: "Email / Phone", width: 30 },
      { key: "detail", header: "Detail", width: 40 },
      { key: "link", header: "HubSpot Link", width: 20 },
    ]
  );

  // Build contactByEmail map for buildSeverityIssues link resolution
  const contactByEmail = new Map();
  for (const c of contacts) {
    const e = c.properties && c.properties.email;
    if (e) contactByEmail.set(e.toLowerCase(), c);
  }
  const tiers = buildSeverityIssues(report, callrailCalls, tickets, contactByEmail);
  const addIssueRow = (tier, item, fillColor) => {
    const row = issuesWs.addRow({
      tier, type: item.type, email: item.email || item.phone || "", detail: item.detail || "", link: "",
    });
    if (fillColor) {
      for (let i = 1; i <= 5; i++) row.getCell(i).fill = { type: "pattern", pattern: "solid", fgColor: { argb: fillColor } };
    }
    // Link to whichever HubSpot record we know about
    if (item.contactId) {
      row.getCell("link").value = { text: `contact #${item.contactId}`, hyperlink: contactUrl(item.contactId) };
      styleLink(row.getCell("link"));
    } else if (item.ticketId) {
      row.getCell("link").value = { text: `ticket #${item.ticketId}`, hyperlink: ticketUrl(item.ticketId) };
      styleLink(row.getCell("link"));
    }
  };
  for (const i of tiers.critical) addIssueRow("Critical", i, TIER_CRITICAL);
  for (const i of tiers.warning) addIssueRow("Warning", i, TIER_WARNING);
  for (const i of tiers.info) addIssueRow("Info", i, null);

  if (tiers.critical.length + tiers.warning.length + tiers.info.length === 0) {
    issuesWs.addRow({ tier: "", type: "No issues this week ✓", email: "", detail: "", link: "" });
  }

  // Write
  const path = require("path");
  const fs = require("fs");
  const dir = path.dirname(outputPath);
  if (dir && dir !== "." && !fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  await wb.xlsx.writeFile(outputPath);
  return outputPath;
}

async function buildXlsx(args, contacts, tickets, formSubmissions, callrailCalls, report, ownersById, salespersonAllocation, inboundsResult, outputPath) {
  const ExcelJS = require("exceljs");
  const portal = process.env.HUBSPOT_PORTAL_ID;
  const contactUrl = (id) => portal ? `https://app.hubspot.com/contacts/${portal}/contact/${id}` : "";
  const ticketUrl = (id) => portal ? `https://app.hubspot.com/contacts/${portal}/ticket/${id}` : "";

  // ── Visual constants (Ledger brand) ─────────────────────────
  const LEDGER_GREEN = "FF2B4A42";       // headers + title
  const LEDGER_GOLD = "FFC49A3C";        // accent (Source row)
  const LEDGER_CREAM = "FFF2EFE9";       // soft background
  const ISSUE_AMBER = "FFFFE699";        // mismatch row highlight
  const LINK_BLUE = "FF0563C1";          // Excel-standard hyperlink color

  // Hyperlink helper — cell value gets both the link AND blue+underline styling.
  // Pass the resulting object to cell.value, then call styleLink(cell) to apply font.
  const linkCell = (text, url) => url
    ? { text: String(text == null ? "" : text), hyperlink: url }
    : String(text == null ? "" : text);
  const styleLink = (cell) => {
    if (cell.value && typeof cell.value === "object" && cell.value.hyperlink) {
      cell.font = { color: { argb: LINK_BLUE }, underline: true };
    }
  };

  const wb = new ExcelJS.Workbook();
  wb.creator = "audit-submissions.js";
  wb.created = new Date();

  // Lookup owner display name (used by tickets/contacts tabs)
  const ownerName = (ownerId) => {
    if (!ownerId) return "(unassigned)";
    const o = ownersById.get(String(ownerId));
    return o ? o.fullName : `Owner ${ownerId}`;
  };

  // Build sets of "issue IDs" so we can highlight referenced rows across tabs
  const issueTicketIds = new Set();
  const issueContactIds = new Set();
  const issueEmails = new Set();
  for (const x of report.webform.mismatches.formSubWithoutTicket) issueEmails.add((x.email || "").toLowerCase());
  for (const x of report.webform.mismatches.contactWithoutTicket) { issueContactIds.add(String(x.contactId)); issueEmails.add((x.email || "").toLowerCase()); }
  for (const x of report.webform.mismatches.duplicateFormSubs) issueEmails.add((x.email || "").toLowerCase());
  for (const x of report.webform.mismatches.multipleTickets) {
    issueTicketIds.add(String(x.firstTicketId));
    issueTicketIds.add(String(x.secondTicketId));
    issueEmails.add((x.email || "").toLowerCase());
  }
  for (const x of report.webform.mismatches.ticketWithoutContact) issueTicketIds.add(String(x.ticketId));
  for (const x of report.callrail.mismatches.duplicateFormSubs) issueEmails.add((x.email || "").toLowerCase());

  // ── Helper: add a tabbed sheet with title + source rows on top ──
  // Layout per tab:
  //   Row 1: TITLE (large, white-on-green, merged across columns)
  //   Row 2: Source: <description> (italic, dark-on-gold, merged)
  //   Row 3: blank (visual separator)
  //   Row 4: column headers (bold white-on-green) ← freeze split here
  //   Row 5+: data
  function addSheetWithTitleAndSource(name, title, sourceText, columns) {
    const ws = wb.addWorksheet(name);
    ws.columns = columns.map(c => ({ ...c, header: undefined }));  // we set headers manually on row 4

    const colCount = columns.length;
    const lastColLetter = String.fromCharCode(64 + colCount);  // assumes <= 26 cols

    // Row 1: title
    ws.mergeCells(`A1:${lastColLetter}1`);
    const titleCell = ws.getCell("A1");
    titleCell.value = title;
    titleCell.font = { name: "Calibri", size: 14, bold: true, color: { argb: "FFFFFFFF" } };
    titleCell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GREEN } };
    titleCell.alignment = { vertical: "middle", horizontal: "left", indent: 1 };
    ws.getRow(1).height = 24;

    // Row 2: Source
    ws.mergeCells(`A2:${lastColLetter}2`);
    const sourceCell = ws.getCell("A2");
    sourceCell.value = `Source: ${sourceText}`;
    sourceCell.font = { name: "Calibri", size: 10, italic: true, color: { argb: LEDGER_GREEN } };
    sourceCell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GOLD } };
    sourceCell.alignment = { vertical: "middle", horizontal: "left", indent: 1 };
    ws.getRow(2).height = 18;

    // Row 3: spacer (blank)
    ws.getRow(3).height = 6;

    // Row 4: column headers
    const headerRow = ws.getRow(4);
    columns.forEach((c, i) => {
      const cell = headerRow.getCell(i + 1);
      cell.value = c.header;
    });
    headerRow.font = { bold: true, color: { argb: "FFFFFFFF" } };
    headerRow.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_GREEN } };
    headerRow.alignment = { vertical: "middle" };
    headerRow.height = 18;

    // Freeze at row 4 so the title/source stay visible too
    ws.views = [{ state: "frozen", ySplit: 4 }];

    // Apply issue-row highlight via helper
    ws._addRowSafe = (data, isIssue) => {
      const row = ws.addRow(data);
      if (isIssue) {
        for (let i = 1; i <= colCount; i++) {
          row.getCell(i).fill = { type: "pattern", pattern: "solid", fgColor: { argb: ISSUE_AMBER } };
        }
      }
      return row;
    };

    return ws;
  }

  // ── Tab: Reading Guide ───────────────────────────────────────
  // Put this FIRST so it's the landing tab when the file opens.
  const guideWs = addSheetWithTitleAndSource(
    "Reading Guide",
    "Reading Guide",
    "How this audit is structured. Each tab is labeled with its primary source (system of record) and role (input vs output).",
    [
      { key: "tab", width: 26 },
      { key: "role", width: 9 },
      { key: "primarySource", width: 22 },
      { key: "pulledFrom", width: 32 },
      { key: "desc", width: 70 },
    ]
  );
  ["Tab", "Role", "Primary Source", "Pulled From (API)", "What it shows"]
    .forEach((h, i) => { guideWs.getRow(4).getCell(i + 1).value = h; });

  // INPUT tabs — sorted by primary source so the lineage is clear
  guideWs.addRow({ tab: "CallRail Calls",         role: "INPUT",  primarySource: "CallRail (DIRECT)",       pulledFrom: "/v3/a/{accountId}/calls.json",                  desc: "Every call CallRail tracked. Includes attribution (source/medium/keywords/landing page) and the forwarded-to number — the closest signal to 'who picked up' since agent_email is empty for all calls (CallRail isn't capturing per-agent identity in your setup)." });
  guideWs.addRow({ tab: "#inbounds Slack Log",    role: "INPUT",  primarySource: "Slack (DIRECT)",          pulledFrom: "Slack conversations.history (#inbounds)",       desc: 'Automated notifications HubSpot and CallRail workflows post to #inbounds for every new form / call. Shows "Assigned To" at notification time — useful cross-reference if HubSpot ticket owner looks wrong (the Slack notification fires off the contact owner, not the ticket owner).' });
  guideWs.addRow({ tab: "Function Errors",        role: "INPUT",  primarySource: "Website (via Slack)",     pulledFrom: "Slack conversations.history (#nervo_ops)",      desc: 'Real-time "submit-form ERROR" alerts posted by netlify/functions/submit-form.js when its catch block fires. THIS IS THE ONLY DATA WE HAVE DIRECTLY FROM THE WEBSITE — everything else in the webform pipeline is HubSpot\'s downstream record. Empty = no function crashes in window.' });
  guideWs.addRow({ tab: "Webform Contacts",       role: "INPUT",  primarySource: "HubSpot",                 pulledFrom: "/crm/v3/objects/contacts/search",               desc: "Contacts with form_source set. These were created or updated by submit-form.js when the function call to HubSpot succeeded. Primary source is HubSpot because this is HubSpot's record of the submission, not the website's." });
  guideWs.addRow({ tab: "Webform Tickets",        role: "INPUT",  primarySource: "HubSpot",                 pulledFrom: "/crm/v3/objects/tickets/search",                desc: 'Tickets with subject starting "Inbound Inquiry" / "Broker Inquiry". Created by submit-form.js after the contact step.' });
  guideWs.addRow({ tab: "Form Submissions",       role: "INPUT",  primarySource: "HubSpot",                 pulledFrom: "/marketing/v3/forms + legacy submissions",      desc: 'HubSpot Forms API submissions. Mostly CallRail\'s "Call Tracking Email" form (when a caller leaves an email). Custom HTML forms have a known quirk where they don\'t always surface here.' });
  guideWs.addRow({ tab: "CallRail Tickets",       role: "INPUT",  primarySource: "HubSpot",                 pulledFrom: "/crm/v3/objects/tickets/search",                desc: 'Tickets with subject starting "Inbound Call". These are HubSpot\'s representation of CallRail events — the underlying event lives in "CallRail Calls" tab. CallRail\'s HubSpot integration creates these automatically.' });
  guideWs.addRow({});
  // OUTPUT tabs
  guideWs.addRow({ tab: "Summary",                role: "OUTPUT", primarySource: "—",                       pulledFrom: "Aggregated from all input tabs",                desc: "Top-line counts and mismatch totals across both pipelines." });
  guideWs.addRow({ tab: "Salesperson Allocation", role: "OUTPUT", primarySource: "—",                       pulledFrom: "Joined by HubSpot owner_id + email",            desc: "Per-salesperson breakdown of tickets / contacts / answered calls. The % column shows share of total tickets in this window." });
  guideWs.addRow({ tab: "Webform Attribution",    role: "OUTPUT", primarySource: "—",                       pulledFrom: "Derived from contact properties",               desc: "Per-contact view of what attribution data was captured (gclid, gad_campaignid, hs_analytics_source). Flags contacts that landed on a paid LP but didn't get full Google attribution. Meta CAPI fires fire-and-forget to Facebook; no per-contact Meta record in HubSpot." });
  guideWs.addRow({ tab: "CallRail Reconciliation",role: "OUTPUT", primarySource: "—",                       pulledFrom: "Joined CallRail Calls ↔ CallRail Tickets by time", desc: "For each CallRail call, finds the matching HubSpot Inbound Call ticket by createdate within ±10 min. Surfaces calls without tickets (CallRail integration broke) and tickets without calls (orphans)." });
  guideWs.addRow({ tab: "Mismatches",             role: "OUTPUT", primarySource: "—",                       pulledFrom: "Cross-referenced webform/callrail inputs",      desc: "Detected anomalies: duplicates, missing matches, orphaned tickets. Rows highlighted amber in input tabs are referenced here." });
  guideWs.addRow({});

  // Prose notes — merged cells across the row
  const noteRow1 = guideWs.addRow({ tab: "── INPUT vs OUTPUT ──" });
  noteRow1.font = { bold: true, color: { argb: LEDGER_GREEN } };
  noteRow1.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
  guideWs.addRow({ tab: "", desc: "INPUT = data observed directly from a system (we just pulled it). OUTPUT = data we computed by joining/aggregating inputs." });
  guideWs.addRow({});
  const noteRow2 = guideWs.addRow({ tab: "── Primary Source meaning ──" });
  noteRow2.font = { bold: true, color: { argb: LEDGER_GREEN } };
  noteRow2.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
  guideWs.addRow({ tab: "", desc: "CallRail (DIRECT) = the system of record; we pull it straight from the origin. HubSpot = HubSpot is just storing it, but the underlying event happened elsewhere (e.g., a CallRail call, a website form submission)." });
  guideWs.addRow({});
  const noteRow3 = guideWs.addRow({ tab: "── Website-direct data gap ──" });
  noteRow3.font = { bold: true, color: { argb: LEDGER_GREEN } };
  noteRow3.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
  guideWs.addRow({ tab: "", desc: "The audit does NOT have direct visibility into webform attempts. submit-form.js doesn't log every attempt anywhere durable (Netlify Functions v1 + Netlify Blobs SDK was tried and didn't bundle). The only website-direct signal is the Function Errors tab — errors that crashed the function. Successful submissions and rejected-by-design ones (honeypot, rate limit, validation fails) are invisible to this audit. If you want full website-side visibility, the path forward is migrating submit-form.js to Functions v2 (Blobs included natively) and adding attempt logging." });
  guideWs.addRow({});
  const noteRow4 = guideWs.addRow({ tab: "── How to spot issues ──" });
  noteRow4.font = { bold: true, color: { argb: LEDGER_GREEN } };
  noteRow4.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
  guideWs.addRow({ tab: "", desc: "Rows highlighted amber are flagged as issues (duplicate, missing match, attribution gap, orphan, etc.). Mismatches tab consolidates them. Blue underlined cells link to the corresponding HubSpot record." });

  // ── Tab: Summary ─────────────────────────────────────────────
  const sumWs = addSheetWithTitleAndSource(
    "Summary",
    "Audit Summary",
    `Computed aggregates for ${args.from} → ${args.to}. See "Reading Guide" tab for how each input maps to these numbers.`,
    [
      { key: "metric", width: 50 },
      { key: "value", width: 20 },
    ]
  );
  const sumHeader = sumWs.getRow(4);
  ["Metric", "Value"].forEach((h, i) => { sumHeader.getCell(i + 1).value = h; });
  const sumSection = (label) => {
    const row = sumWs.addRow({ metric: label, value: "" });
    row.font = { bold: true, color: { argb: LEDGER_GREEN } };
    row.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
  };
  sumWs.addRow({ metric: "Audit window", value: `${args.from} → ${args.to}` });
  sumWs.addRow({ metric: "Generated at", value: new Date().toISOString() });
  sumWs.addRow({});
  sumSection("Webform pipeline");
  sumWs.addRow({ metric: "Contacts with form_source set", value: report.webform.counts.contacts });
  sumWs.addRow({ metric: "Tickets created", value: report.webform.counts.tickets });
  sumWs.addRow({ metric: "HubSpot form submissions", value: report.webform.counts.formSubmissions });
  sumWs.addRow({ metric: "submit-form ERROR alerts (Slack)", value: report.functionErrors ? report.functionErrors.errors.length : "(layer unavailable)" });
  sumWs.addRow({});
  sumSection("Webform mismatches");
  sumWs.addRow({ metric: "Form sub without matching ticket", value: report.webform.mismatches.formSubWithoutTicket.length });
  sumWs.addRow({ metric: "Contact with form_source but no ticket", value: report.webform.mismatches.contactWithoutTicket.length });
  sumWs.addRow({ metric: "Duplicate form submissions (5min)", value: report.webform.mismatches.duplicateFormSubs.length });
  sumWs.addRow({ metric: "Duplicate tickets (5min)", value: report.webform.mismatches.multipleTickets.length });
  sumWs.addRow({ metric: "Tickets without contact assoc", value: report.webform.mismatches.ticketWithoutContact.length });
  sumWs.addRow({});
  sumSection("CallRail pipeline");
  sumWs.addRow({ metric: "CallRail calls (total)", value: report.callrail.counts.calls !== null ? report.callrail.counts.calls : "(layer unavailable)" });
  sumWs.addRow({ metric: "CallRail calls (answered)", value: report.callrail.counts.answeredCalls !== null ? report.callrail.counts.answeredCalls : "(n/a)" });
  sumWs.addRow({ metric: 'HubSpot "Inbound Call" tickets', value: report.callrail.counts.tickets });
  sumWs.addRow({ metric: 'HubSpot "Call Tracking Email" submissions', value: report.callrail.counts.formSubmissions });
  sumWs.addRow({});
  sumSection("CallRail mismatches");
  sumWs.addRow({ metric: "Answered calls without nearby ticket", value: report.callrail.mismatches.callsWithoutTicket.length });
  sumWs.addRow({ metric: "Duplicate Call Tracking Email subs (5min)", value: report.callrail.mismatches.duplicateFormSubs.length });

  // ── Tab: Salesperson Allocation ──────────────────────────────
  const salesWs = addSheetWithTitleAndSource(
    "Salesperson Allocation",
    "Salesperson Allocation",
    "Tickets and calls grouped by HubSpot owner / CallRail agent_email. Joined by email when the same person appears in both systems.",
    [
      { key: "name", width: 28 },
      { key: "email", width: 30 },
      { key: "webformTickets", width: 18 },
      { key: "callrailTickets", width: 18 },
      { key: "totalTickets", width: 16 },
      { key: "pctOfTickets", width: 14 },
      { key: "contactsOwned", width: 18 },
      { key: "callsAnswered", width: 18 },
    ]
  );
  const salesHeader = salesWs.getRow(4);
  ["Salesperson", "Email", "Webform Tickets", "CallRail Tickets", "Total Tickets", "% of Total", "Contacts Owned", "Calls Answered"]
    .forEach((h, i) => { salesHeader.getCell(i + 1).value = h; });
  for (const r of salespersonAllocation.rows) {
    const row = salesWs.addRow({
      name: r.name,
      email: r.email,
      webformTickets: r.webformTickets,
      callrailTickets: r.callrailTickets,
      totalTickets: r.totalTickets,
      pctOfTickets: r.pctOfTickets,
      contactsOwned: r.contactsOwned,
      callsAnswered: r.callsAnswered,
    });
    row.getCell("pctOfTickets").numFmt = "0.0%";
    // Gold tint for "(unassigned)" or "Owner <id>" buckets so they stand out
    if (r.key.startsWith("unassigned") || r.key.startsWith("unknown:")) {
      for (let i = 1; i <= 8; i++) {
        row.getCell(i).fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
        row.getCell(i).font = { italic: true };
      }
    }
  }

  // ── Tab: Webform Tickets ─────────────────────────────────────
  const wtxWs = addSheetWithTitleAndSource(
    "Webform Tickets",
    "Webform Tickets",
    'HubSpot Tickets API — subjects starting with "Inbound Inquiry" / "Broker Inquiry". Created by netlify/functions/submit-form.js. "Owner Mismatch" column = TRUE when the ticket is assigned to a different person (or unassigned) than the contact.',
    [
      { key: "id", width: 14 },
      { key: "subject", width: 50 },
      { key: "category", width: 22 },
      { key: "created", width: 22 },
      { key: "email", width: 30 },
      { key: "ticketOwner", width: 22 },
      { key: "contactOwner", width: 22 },
      { key: "mismatch", width: 12 },
      { key: "contactId", width: 14 },
    ]
  );
  ["Ticket ID", "Subject", "Category", "Created", "Email", "Ticket Owner", "Contact Owner", "Owner Mismatch", "Contact ID"]
    .forEach((h, i) => { wtxWs.getRow(4).getCell(i + 1).value = h; });
  for (const t of tickets) {
    if (classifyTicket(t) !== "webform") continue;
    const ticketOwnerId = t.properties.hubspot_owner_id || "";
    const contactOwnerId = t._contactOwnerId || "";
    const ownerMismatch = ticketOwnerId !== contactOwnerId && (ticketOwnerId || contactOwnerId);
    const isIssue = ownerMismatch || issueTicketIds.has(String(t.id)) || (t._email && issueEmails.has(t._email));
    const row = wtxWs._addRowSafe({
      id: t.id,
      subject: t.properties.subject || "",
      category: t.properties.hs_ticket_category || "",
      created: t.properties.createdate || "",
      email: t._email || "",
      ticketOwner: ownerName(ticketOwnerId),
      contactOwner: ownerName(contactOwnerId),
      mismatch: ownerMismatch ? "YES" : "",
      contactId: t._contactId || "",
    }, isIssue);
    row.getCell("id").value = linkCell(`#${t.id}`, ticketUrl(t.id));
    styleLink(row.getCell("id"));
    if (t._contactId) {
      row.getCell("contactId").value = linkCell(t._contactId, contactUrl(t._contactId));
      styleLink(row.getCell("contactId"));
    }
  }

  // ── Tab: Webform Contacts ────────────────────────────────────
  const wcWs = addSheetWithTitleAndSource(
    "Webform Contacts",
    "Webform Contacts",
    "HubSpot Contacts API — filtered to contacts with form_source set (touched by submit-form.js).",
    [
      { key: "id", width: 14 },
      { key: "email", width: 30 },
      { key: "firstName", width: 16 },
      { key: "lastName", width: 16 },
      { key: "formSource", width: 32 },
      { key: "adCampaign", width: 18 },
      { key: "created", width: 22 },
      { key: "ownerName", width: 22 },
    ]
  );
  ["Contact ID", "Email", "First Name", "Last Name", "Form Source", "Ad Campaign", "Created", "Owner"]
    .forEach((h, i) => { wcWs.getRow(4).getCell(i + 1).value = h; });
  for (const c of contacts) {
    const p = c.properties || {};
    const isIssue = issueContactIds.has(String(c.id)) || (p.email && issueEmails.has(p.email.toLowerCase()));
    const row = wcWs._addRowSafe({
      id: c.id,
      email: p.email || "",
      firstName: p.firstname || "",
      lastName: p.lastname || "",
      formSource: p.form_source || "",
      adCampaign: p.ad_campaign || "",
      created: p.createdate || "",
      ownerName: ownerName(p.hubspot_owner_id),
    }, isIssue);
    row.getCell("id").value = linkCell(c.id, contactUrl(c.id));
    styleLink(row.getCell("id"));
  }

  // ── Tab: Webform Attribution ─────────────────────────────────
  // Per-contact view of what attribution data we captured (Google Ads, UTM,
  // hutk, analytics source). Surfaces gaps so we can see when expected
  // attribution didn't make it onto a contact.
  const attrWs = addSheetWithTitleAndSource(
    "Webform Attribution",
    "Webform Attribution & Enrichment",
    'OUTPUT — per webform contact, what attribution data submit-form.js managed to capture from the website request. Note: Meta CAPI events fire fire-and-forget to Facebook; no per-contact Meta status is stored in HubSpot.',
    [
      { key: "id", width: 14 },
      { key: "email", width: 30 },
      { key: "formSource", width: 32 },
      { key: "adCampaign", width: 18 },
      { key: "analyticsSource", width: 18 },
      { key: "sourceData1", width: 22 },
      { key: "sourceData2", width: 22 },
      { key: "gclid", width: 16 },
      { key: "gadCampaignId", width: 18 },
      { key: "status", width: 28 },
    ]
  );
  [
    "Contact ID", "Email", "Form Source", "Ad Campaign (derived)",
    "HS Analytics Source", "Source Data 1", "Source Data 2",
    "gclid captured?", "Google Ads campaign_id?", "Attribution Status",
  ].forEach((h, i) => { attrWs.getRow(4).getCell(i + 1).value = h; });

  for (const c of contacts) {
    const p = c.properties || {};
    const formSource = (p.form_source || "").toLowerCase();
    const isPaidLp = formSource.endsWith("-google-ads") || formSource.includes("google-ads");
    const hasGclid = !!p.hs_google_click_id;
    const hasCampaign = !!p.gad_campaignid;
    let status;
    if (isPaidLp && hasGclid && hasCampaign) status = "✓ Google Ads (full)";
    else if (isPaidLp && (hasGclid || hasCampaign)) status = "⚠ Google Ads (partial)";
    else if (isPaidLp) status = "⚠ Google Ads LP, no click ID captured";
    else if (p.hs_analytics_source === "DIRECT_TRAFFIC") status = "Direct";
    else if (p.hs_analytics_source === "ORGANIC_SEARCH") status = "Organic search";
    else if (p.hs_analytics_source === "REFERRALS") status = "Referral";
    else if (p.hs_analytics_source) status = p.hs_analytics_source;
    else status = "(no attribution)";
    const isIssue = status.startsWith("⚠");

    const row = attrWs._addRowSafe({
      id: c.id,
      email: p.email || "",
      formSource: p.form_source || "",
      adCampaign: p.ad_campaign || "",
      analyticsSource: p.hs_analytics_source || "",
      sourceData1: p.hs_analytics_source_data_1 || "",
      sourceData2: p.hs_analytics_source_data_2 || "",
      gclid: hasGclid ? (p.hs_google_click_id.slice(0, 8) + "…") : "—",
      gadCampaignId: p.gad_campaignid || "—",
      status,
    }, isIssue);
    row.getCell("id").value = linkCell(c.id, contactUrl(c.id));
    styleLink(row.getCell("id"));
  }

  // ── Tab: Form Submissions ────────────────────────────────────
  const fsWs = addSheetWithTitleAndSource(
    "Form Submissions",
    "HubSpot Form Submissions",
    "HubSpot Forms API — /marketing/v3/forms + /form-integrations/v1/submissions. Mostly CallRail's Call Tracking Email.",
    [
      { key: "formName", width: 32 },
      { key: "pipeline", width: 12 },
      { key: "email", width: 30 },
      { key: "submittedAt", width: 22 },
      { key: "pageUrl", width: 60 },
    ]
  );
  ["Form Name", "Pipeline", "Email", "Submitted", "Page URL"]
    .forEach((h, i) => { fsWs.getRow(4).getCell(i + 1).value = h; });
  for (const f of formSubmissions) {
    const isIssue = f.email && issueEmails.has(f.email.toLowerCase());
    fsWs._addRowSafe({
      formName: f.formName || "",
      pipeline: classifyFormSub(f),
      email: f.email || "",
      submittedAt: f.submittedAt || "",
      pageUrl: f.pageUrl || "",
    }, isIssue);
  }

  // ── Tab: CallRail Calls ──────────────────────────────────────
  const crWs = addSheetWithTitleAndSource(
    "CallRail Calls",
    "CallRail Calls",
    'PRIMARY SOURCE — pulled DIRECTLY from CallRail API (/v3/a/{accountId}/calls.json). This is the system of record for phone calls; everything HubSpot knows about calls is downstream of this.',
    [
      { key: "id", width: 16 },
      { key: "startTime", width: 22 },
      { key: "answered", width: 10 },
      { key: "duration", width: 12 },
      { key: "phone", width: 18 },
      { key: "name", width: 22 },
      { key: "city", width: 12 },
      { key: "state", width: 8 },
      { key: "source", width: 16 },
      { key: "medium", width: 10 },
      { key: "keywords", width: 36 },
      { key: "landingPage", width: 60 },
      { key: "trackingPhone", width: 16 },
      { key: "forwardedTo", width: 16 },
      { key: "answeredBy", width: 14 },
      { key: "recording", width: 18 },
    ]
  );
  [
    "Call ID", "Start Time", "Answered", "Duration (s)", "Customer Phone", "Customer Name",
    "City", "State", "Source", "Medium", "Keywords", "Landing Page URL", "Tracking #",
    "Forwarded To", "Answered By", "Recording",
  ].forEach((h, i) => { crWs.getRow(4).getCell(i + 1).value = h; });
  for (const c of callrailCalls) {
    const answeredBy = phoneToName(c.businessPhone);
    const row = crWs._addRowSafe({
      id: c.id,
      startTime: c.startTime || "",
      answered: c.answered ? "Y" : "N",
      duration: c.duration || 0,
      phone: c.customerPhone || "",
      name: c.customerName || "",
      city: c.customerCity || "",
      state: c.customerState || "",
      source: c.source || "",
      medium: c.medium || "",
      keywords: c.keywords || "",
      landingPage: c.landingPageUrl || "",
      trackingPhone: c.trackingPhone || "",
      forwardedTo: c.businessPhone || "",
      answeredBy: answeredBy || (c.answered ? "(unmapped #)" : ""),
      recording: c.recordingPlayer ? "Listen" : "",
    }, false);
    if (c.recordingPlayer) {
      row.getCell("recording").value = linkCell("Listen", c.recordingPlayer);
      styleLink(row.getCell("recording"));
    }
    if (c.landingPageUrl) {
      row.getCell("landingPage").value = linkCell(c.landingPageUrl, c.landingPageUrl);
      styleLink(row.getCell("landingPage"));
    }
  }

  // ── Tab: CallRail Tickets ────────────────────────────────────
  const ctxWs = addSheetWithTitleAndSource(
    "CallRail Tickets",
    "CallRail Tickets",
    'HubSpot Tickets API — subjects starting with "Inbound Call". Auto-created by CallRail\'s HubSpot integration. "Answered By" is derived from the matching call\'s forwarded-to number (see CallRail Reconciliation tab).',
    [
      { key: "id", width: 14 },
      { key: "subject", width: 50 },
      { key: "created", width: 22 },
      { key: "email", width: 30 },
      { key: "ticketOwner", width: 22 },
      { key: "contactOwner", width: 22 },
      { key: "answeredBy", width: 14 },
      { key: "mismatch", width: 12 },
    ]
  );
  ["Ticket ID", "Subject", "Created", "Email (if assoc)", "Ticket Owner", "Contact Owner", "Answered By (call)", "Owner Mismatch"]
    .forEach((h, i) => { ctxWs.getRow(4).getCell(i + 1).value = h; });

  // Build a quick lookup: ticket.id → matched CallRail call (for answeredBy)
  // We rebuild the same time-match logic that CallRail Reconciliation uses.
  const ticketToCall = new Map();
  {
    const sortedCalls = [...callrailCalls].sort((a, b) => new Date(a.startTime) - new Date(b.startTime));
    for (const t of tickets) {
      if (classifyTicket(t) !== "callrail") continue;
      const tMs = new Date(t.properties.createdate).getTime();
      let best = null;
      for (const call of sortedCalls) {
        const gap = Math.abs(new Date(call.startTime).getTime() - tMs);
        if (gap <= 10 * 60 * 1000 && (!best || gap < best.gap)) best = { call, gap };
      }
      if (best) ticketToCall.set(t.id, best.call);
    }
  }

  for (const t of tickets) {
    if (classifyTicket(t) !== "callrail") continue;
    const ticketOwnerId = t.properties.hubspot_owner_id || "";
    const contactOwnerId = t._contactOwnerId || "";
    const ownerMismatch = ticketOwnerId !== contactOwnerId && (ticketOwnerId || contactOwnerId);
    const matchedCall = ticketToCall.get(t.id);
    const answeredBy = matchedCall ? phoneToName(matchedCall.businessPhone) : "";
    const isIssue = ownerMismatch || issueTicketIds.has(String(t.id));
    const row = ctxWs._addRowSafe({
      id: t.id,
      subject: t.properties.subject || "",
      created: t.properties.createdate || "",
      email: t._email || "",
      ticketOwner: ownerName(ticketOwnerId),
      contactOwner: ownerName(contactOwnerId),
      answeredBy: answeredBy || (matchedCall ? "(unmapped #)" : "(no matched call)"),
      mismatch: ownerMismatch ? "YES" : "",
    }, isIssue);
    row.getCell("id").value = linkCell(`#${t.id}`, ticketUrl(t.id));
    styleLink(row.getCell("id"));
  }

  // ── Tab: CallRail Reconciliation ─────────────────────────────
  // Joins each CallRail call (primary source) to the HubSpot "Inbound Call"
  // ticket created by CallRail's integration. Time-based match within ±10 min.
  const crReconWs = addSheetWithTitleAndSource(
    "CallRail Reconciliation",
    "CallRail ↔ HubSpot Ticket Reconciliation",
    'OUTPUT — joins CallRail Calls (PRIMARY) to "Inbound Call" tickets in HubSpot by createdate proximity (±10 min). Surfaces calls that didn\'t produce a ticket, or tickets that don\'t correspond to a known call.',
    [
      { key: "callId", width: 28 },
      { key: "callTime", width: 22 },
      { key: "answered", width: 10 },
      { key: "phone", width: 18 },
      { key: "name", width: 22 },
      { key: "forwardedTo", width: 16 },
      { key: "answeredBy", width: 14 },
      { key: "ticketId", width: 14 },
      { key: "ticketTime", width: 22 },
      { key: "gapSeconds", width: 12 },
      { key: "status", width: 18 },
    ]
  );
  ["Call ID", "Call Time", "Answered", "Phone", "Customer Name", "Forwarded To", "Answered By", "Matched Ticket", "Ticket Time", "Gap (s)", "Match Status"]
    .forEach((h, i) => { crReconWs.getRow(4).getCell(i + 1).value = h; });

  // Build a lookup of CallRail-classified tickets sorted by createdate
  const callrailTickets = tickets
    .filter(t => classifyTicket(t) === "callrail")
    .map(t => ({ ...t, _createMs: new Date(t.properties.createdate).getTime() }))
    .sort((a, b) => a._createMs - b._createMs);

  const matchWindowMs = 10 * 60 * 1000;
  const matchedTicketIds = new Set();

  for (const call of callrailCalls) {
    const callMs = new Date(call.startTime).getTime();
    // Find candidates within window
    const candidates = callrailTickets
      .filter(t => Math.abs(t._createMs - callMs) <= matchWindowMs)
      .map(t => ({ t, gap: t._createMs - callMs }));
    candidates.sort((a, b) => Math.abs(a.gap) - Math.abs(b.gap));
    const best = candidates[0];
    const matched = !!best;
    const status = matched
      ? (candidates.length > 1 ? "matched (best of " + candidates.length + ")" : "matched")
      : (call.answered ? "no-match (answered)" : "no-match (missed)");

    if (matched) matchedTicketIds.add(best.t.id);

    const isIssue = !matched && call.answered;  // answered calls without tickets are issues
    const answeredBy = phoneToName(call.businessPhone);
    const row = crReconWs._addRowSafe({
      callId: call.id,
      callTime: call.startTime,
      answered: call.answered ? "Y" : "N",
      phone: call.customerPhone || "",
      name: call.customerName || "",
      forwardedTo: call.businessPhone || "",
      answeredBy: answeredBy || (call.answered ? "(unmapped #)" : ""),
      ticketId: matched ? `#${best.t.id}` : "",
      ticketTime: matched ? best.t.properties.createdate : "",
      gapSeconds: matched ? Math.round(best.gap / 1000) : "",
      status,
    }, isIssue);
    if (matched) {
      row.getCell("ticketId").value = linkCell(`#${best.t.id}`, ticketUrl(best.t.id));
      styleLink(row.getCell("ticketId"));
    }
  }

  // Now add a second block: orphan tickets (CallRail tickets with no matching call)
  const orphanTickets = callrailTickets.filter(t => !matchedTicketIds.has(t.id));
  if (orphanTickets.length) {
    const spacer = crReconWs.addRow({});
    const banner = crReconWs.addRow({ callId: `── Orphan tickets (${orphanTickets.length}): "Inbound Call" tickets without a matching CallRail call ──` });
    banner.font = { bold: true, color: { argb: LEDGER_GREEN } };
    banner.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
    for (const t of orphanTickets) {
      const row = crReconWs._addRowSafe({
        callId: "",
        callTime: "",
        answered: "",
        phone: "",
        name: "",
        forwardedTo: "",
        answeredBy: "",
        ticketId: `#${t.id}`,
        ticketTime: t.properties.createdate,
        gapSeconds: "",
        status: "orphan ticket",
      }, true);
      row.getCell("ticketId").value = linkCell(`#${t.id}`, ticketUrl(t.id));
      styleLink(row.getCell("ticketId"));
    }
  }

  // ── Tab: Mismatches ──────────────────────────────────────────
  const mmWs = addSheetWithTitleAndSource(
    "Mismatches",
    "Detected Mismatches",
    "Computed cross-references across all input tabs. Rows here are highlighted amber in their source tabs too.",
    [
      { key: "type", width: 38 },
      { key: "pipeline", width: 10 },
      { key: "email", width: 30 },
      { key: "detail", width: 60 },
      { key: "time", width: 24 },
      { key: "link1", width: 18 },
      { key: "link2", width: 18 },
    ]
  );
  ["Type", "Pipeline", "Email / Phone", "Detail", "Time", "Link 1", "Link 2"]
    .forEach((h, i) => { mmWs.getRow(4).getCell(i + 1).value = h; });
  const wm = report.webform.mismatches;
  const addMismatchRow = (data, links = {}) => {
    const row = mmWs._addRowSafe(data, true);
    if (links.link1) { row.getCell("link1").value = linkCell(links.link1.text, links.link1.url); styleLink(row.getCell("link1")); }
    if (links.link2) { row.getCell("link2").value = linkCell(links.link2.text, links.link2.url); styleLink(row.getCell("link2")); }
  };
  for (const x of wm.formSubWithoutTicket) {
    addMismatchRow({ type: "Form sub without ticket", pipeline: "webform", email: x.email, detail: x.formName, time: x.submittedAt });
  }
  for (const x of wm.contactWithoutTicket) {
    addMismatchRow({ type: "Contact with form_source but no ticket", pipeline: "webform", email: x.email, detail: x.formSource, time: x.createdAt },
      { link1: { text: `contact ${x.contactId}`, url: contactUrl(x.contactId) } });
  }
  for (const x of wm.duplicateFormSubs) {
    addMismatchRow({ type: "Duplicate form submissions (5min)", pipeline: "webform", email: x.email, detail: x.formName, time: `${x.firstAt} & ${x.secondAt}` });
  }
  for (const x of wm.multipleTickets) {
    addMismatchRow({ type: "Duplicate tickets (5min)", pipeline: "webform", email: x.email, detail: `gap ${x.gapSeconds}s` },
      { link1: { text: `#${x.firstTicketId}`, url: ticketUrl(x.firstTicketId) },
        link2: { text: `#${x.secondTicketId}`, url: ticketUrl(x.secondTicketId) } });
  }
  for (const x of wm.ticketWithoutContact) {
    addMismatchRow({ type: "Ticket without contact assoc", pipeline: "webform", detail: x.subject, time: x.createdAt },
      { link1: { text: `#${x.ticketId}`, url: ticketUrl(x.ticketId) } });
  }
  const cm = report.callrail.mismatches;
  for (const x of cm.callsWithoutTicket) {
    addMismatchRow({ type: "Answered call without nearby ticket", pipeline: "callrail", email: x.phone, detail: `${x.name || ""} ${x.city || ""} src=${x.source || ""}`.trim(), time: x.startTime });
  }
  for (const x of cm.duplicateFormSubs) {
    addMismatchRow({ type: "Duplicate Call Tracking Email (5min)", pipeline: "callrail", email: x.email, time: `${x.firstAt} & ${x.secondAt}` });
  }

  // ── Tab: #inbounds Slack Log ─────────────────────────────────
  if (inboundsResult && inboundsResult.accessible) {
    const inboundsWs = addSheetWithTitleAndSource(
      "#inbounds Slack Log",
      "#inbounds Slack notifications",
      'PRIMARY SOURCE — Slack #inbounds channel history. The HubSpot/CallRail workflow auto-posts here on every new contact/call, with the assignee. Useful cross-reference: "who was actually notified" vs ticket/contact owner in HubSpot.',
      [
        { key: "time", width: 22 },
        { key: "type", width: 8 },
        { key: "name", width: 22 },
        { key: "assignee", width: 16 },
        { key: "email", width: 28 },
        { key: "phone", width: 18 },
        { key: "channel", width: 18 },
        { key: "page", width: 30 },
        { key: "keyword", width: 36 },
      ]
    );
    ["Time", "Type", "Customer Name", "Assigned To", "Email", "Phone", "Channel", "Landing Page", "Keyword"]
      .forEach((h, i) => { inboundsWs.getRow(4).getCell(i + 1).value = h; });

    // Filter to just the automated notifications (drop human chatter)
    const automated = inboundsResult.entries.filter(e => e.type === "form" || e.type === "call");
    for (const e of automated) {
      inboundsWs._addRowSafe({
        time: e.isoTime,
        type: e.type,
        name: e.customerName,
        assignee: e.assignee,
        email: e.email,
        phone: e.phone,
        channel: e.channel,
        page: e.page,
        keyword: e.keyword,
      }, false);
    }

    // Drop note about excluded human messages
    const humanCount = inboundsResult.entries.filter(e => e.type === "other").length;
    if (humanCount > 0) {
      inboundsWs.addRow({});
      const note = inboundsWs.addRow({ time: `── ${humanCount} non-automated human messages also in channel (not shown — chat context, not lead data) ──` });
      note.font = { italic: true, color: { argb: LEDGER_GREEN } };
      note.fill = { type: "pattern", pattern: "solid", fgColor: { argb: LEDGER_CREAM } };
    }
  }

  // ── Tab: submit-form Errors ──────────────────────────────────
  if (report.functionErrors && report.functionErrors.accessible) {
    const erWs = addSheetWithTitleAndSource(
      "Function Errors",
      "submit-form Function Errors",
      'Slack #nervo_ops history — messages containing "submit-form ERROR" marker, posted by the function\'s catch block.',
      [
        { key: "time", width: 24 },
        { key: "text", width: 120 },
      ]
    );
    ["Time", "Message (from Slack)"].forEach((h, i) => { erWs.getRow(4).getCell(i + 1).value = h; });
    for (const e of report.functionErrors.errors) {
      erWs._addRowSafe({ time: e.isoTime, text: e.text }, true);
    }
  }

  // Ensure output directory exists
  const path = require("path");
  const fs = require("fs");
  const dir = path.dirname(outputPath);
  if (dir && dir !== "." && !fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  await wb.xlsx.writeFile(outputPath);
  return outputPath;
}

// ─── Main ─────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs(process.argv);
  const fromIso = `${args.from}T00:00:00.000Z`;
  const toIso = `${args.to}T23:59:59.999Z`;

  console.log(`Audit window: ${args.from} → ${args.to}`);
  console.log(`Mode: ${args.dryRun ? "dry-run (stdout only)" : "Slack post"}\n`);

  console.log("Fetching HubSpot owners…");
  const ownersResult = await fetchOwners();
  console.log(`  ${ownersResult.ownersById.size} owners` + (ownersResult.accessible ? "" : " (layer unavailable)"));

  console.log("Fetching contacts…");
  const contacts = await fetchContacts(fromIso, toIso);
  console.log(`  ${contacts.length} contacts`);

  console.log("Fetching tickets (with contact associations)…");
  const tickets = await fetchTickets(fromIso, toIso);
  console.log(`  ${tickets.length} tickets`);

  console.log("Fetching HubSpot form submissions…");
  const formsResult = await fetchFormSubmissions(fromIso, toIso);
  console.log(`  ${formsResult.submissions.length} form submissions (across ${formsResult.formsScanned} forms)`);

  console.log("Fetching CallRail calls…");
  const callrailResult = await fetchCallRailCalls(fromIso, toIso);
  console.log(`  ${callrailResult.calls.length} calls` + (callrailResult.accessible ? "" : " (layer unavailable)"));

  console.log("Fetching Slack error alerts…");
  const slackErrorsResult = await fetchSlackErrors(fromIso, toIso);
  console.log(`  ${slackErrorsResult.errors.length} submit-form ERROR alerts in window` + (slackErrorsResult.accessible ? "" : " (layer unavailable)"));

  console.log("Fetching #inbounds log…");
  const inboundsResult = await fetchInboundsLog(fromIso, toIso);
  console.log(`  ${inboundsResult.entries.length} #inbounds messages` + (inboundsResult.accessible ? "" : " (layer unavailable)"));

  console.log("Fetching raw submission attempts…");
  const rawSubmissionsResult = await fetchRawSubmissions(fromIso, toIso);
  console.log(`  ${rawSubmissionsResult.attempts.length} attempts logged` + (rawSubmissionsResult.accessible ? "" : " (layer unavailable)"));

  // Pre-resolve a HubSpot contact ID for each CallRail call:
  // 1. Try matching to a CallRail ticket within ±10min (preferred — captures
  //    the integration's own association)
  // 2. Fall back to searching HubSpot directly by phone (CONTAINS_TOKEN on
  //    last-10-digits) so even recent calls without a ticket-association land
  //    a contact link in the report
  console.log("Resolving call → contact links…");
  {
    const sortedCallrailTickets = tickets
      .filter(t => classifyTicket(t) === "callrail")
      .map(t => ({ t, ms: new Date(t.properties.createdate).getTime() }))
      .sort((a, b) => a.ms - b.ms);
    let timeMatched = 0, phoneMatched = 0, unresolved = 0;
    for (const call of callrailResult.calls) {
      const callMs = new Date(call.startTime).getTime();
      let best = null;
      for (const { t, ms } of sortedCallrailTickets) {
        const gap = Math.abs(ms - callMs);
        if (gap <= 10 * 60 * 1000 && (!best || gap < best.gap)) best = { t, gap };
      }
      if (best && best.t._contactId) {
        call._resolvedContactId = best.t._contactId;
        timeMatched++;
      } else {
        const c = await getContactByPhone(call.customerPhone);
        if (c) { call._resolvedContactId = c.id; phoneMatched++; }
        else { call._resolvedContactId = null; unresolved++; }
      }
    }
    console.log(`  ${timeMatched} via ticket, ${phoneMatched} via phone search, ${unresolved} unresolved`);
  }

  console.log("\nReconciling…");
  const report = reconcile(contacts, tickets, formsResult.submissions, callrailResult.calls);
  report.functionErrors = slackErrorsResult;

  // Default Slack output = the concise one-pager. Pass --detailed-slack to use
  // the legacy multi-pipeline format (kept for ad-hoc deep dives).
  const payload = args.detailedSlack
    ? buildSlackPayload(report, args, formsResult.accessible, callrailResult.accessible)
    : buildOnePagerSlackPayload(report, args, contacts, tickets, callrailResult.calls, rawSubmissionsResult, ownersResult.ownersById);

  // XLSX export (writes file; does not affect Slack behavior on its own)
  if (args.xlsx !== null) {
    // Include HHMM suffix so re-runs don't fail when the previous file is open
    const now = new Date();
    const stamp = `${String(now.getHours()).padStart(2, "0")}${String(now.getMinutes()).padStart(2, "0")}`;
    const flavor = args.xlsxDetailed ? "detailed" : "lightweight";
    const xlsxPath = args.xlsx || `reports/audit-${args.from}-to-${args.to}-${stamp}-${flavor}.xlsx`;
    console.log(`Writing ${flavor} XLSX → ${xlsxPath}`);
    let finalPath;
    if (args.xlsxDetailed) {
      const salespersonAllocation = buildSalespersonAllocation(contacts, tickets, callrailResult.calls, ownersResult.ownersById);
      finalPath = await buildXlsx(args, contacts, tickets, formsResult.submissions, callrailResult.calls, report, ownersResult.ownersById, salespersonAllocation, inboundsResult, xlsxPath);
    } else {
      finalPath = await buildLightweightXlsx(args, contacts, tickets, callrailResult.calls, report, ownersResult.ownersById, xlsxPath);
    }
    console.log(`  Wrote ${finalPath}`);
  }

  // Slack: --dry-run suppresses posting; otherwise post (regardless of --xlsx).
  if (args.dryRun) {
    console.log("\n─── Slack payload (dry-run) ───\n");
    console.log(payload.text);
  } else {
    console.log("Posting to Slack…");
    await postSlack(payload, args.slackChannel);
    console.log("Done.");
  }
}

main().catch(err => {
  console.error("\nAudit failed:", err.message);
  if (err.stack) console.error(err.stack);
  process.exit(1);
});
