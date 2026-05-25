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
//   CALLRAIL_API_KEY       — optional. Enables Layer 4: CallRail calls vs HubSpot reconciliation
//   CALLRAIL_ACCOUNT_ID    — optional. CallRail account ID

const HUBSPOT_API = "https://api.hubapi.com";

// ─── CLI arg parsing ────────────────────────────────────────────────
function parseArgs(argv) {
  const args = { dryRun: false, weekly: false, from: null, to: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") args.dryRun = true;
    else if (a === "--weekly") args.weekly = true;
    else if (a === "--from") args.from = argv[++i];
    else if (a === "--to") args.to = argv[++i];
    else if (a === "--help" || a === "-h") {
      console.log("Usage: node scripts/audit-submissions.js [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--weekly] [--dry-run]");
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

// ─── Layer 1: Contacts created with form_source set ────────────────
async function fetchContacts(fromIso, toIso) {
  const filters = [
    { propertyName: "createdate", operator: "GTE", value: fromIso },
    { propertyName: "createdate", operator: "LTE", value: toIso },
    { propertyName: "form_source", operator: "HAS_PROPERTY" },
  ];
  return searchAll("contacts", filters, [
    "email", "firstname", "lastname", "form_source", "ad_campaign",
    "createdate", "hubspot_owner_id", "hs_analytics_source",
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

  // Fetch associated contact for each ticket (batched)
  const enriched = [];
  for (const t of tickets) {
    const assocs = await hubspot("GET", `/crm/v4/objects/tickets/${t.id}/associations/contacts`);
    const contactId = assocs.results && assocs.results[0] ? assocs.results[0].toObjectId : null;
    let email = null;
    if (contactId) {
      try {
        const c = await hubspot("GET", `/crm/v3/objects/contacts/${contactId}?properties=email`);
        email = c.properties && c.properties.email ? c.properties.email.toLowerCase() : null;
      } catch { /* ignore — contact may have been deleted */ }
    }
    enriched.push({ ...t, _contactId: contactId, _email: email });
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
      fields: "answered,direction,duration,start_time,customer_phone_number,customer_name,customer_city,tracking_phone_number,source",
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
      trackingPhone: c.tracking_phone_number || "",
      source: c.source || "",
    })));
    if (calls.length < 250) break;
    page++;
  }
  return { calls: all, accessible: true };
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

async function postSlack(payload) {
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

// ─── Main ─────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs(process.argv);
  const fromIso = `${args.from}T00:00:00.000Z`;
  const toIso = `${args.to}T23:59:59.999Z`;

  console.log(`Audit window: ${args.from} → ${args.to}`);
  console.log(`Mode: ${args.dryRun ? "dry-run (stdout only)" : "Slack post"}\n`);

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

  console.log("\nReconciling…");
  const report = reconcile(contacts, tickets, formsResult.submissions, callrailResult.calls);
  report.functionErrors = slackErrorsResult;

  const payload = buildSlackPayload(report, args, formsResult.accessible, callrailResult.accessible);

  if (args.dryRun) {
    console.log("\n─── Slack payload (dry-run) ───\n");
    console.log(payload.text);
  } else {
    console.log("Posting to Slack…");
    await postSlack(payload);
    console.log("Done.");
  }
}

main().catch(err => {
  console.error("\nAudit failed:", err.message);
  if (err.stack) console.error(err.stack);
  process.exit(1);
});
