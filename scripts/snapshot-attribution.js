#!/usr/bin/env node
// Ledger Trade & Capital — Contact Attribution Snapshot
//
// Prints a contact's attribution + form-mapped properties WITH their full
// value history (HubSpot `propertiesWithHistory`), so you can see the SOURCE
// of each value: FORM / API / ANALYTICS / INTEGRATION / CRM_UI etc.
//
// Why this exists:
//   When the ~146 "Unregistered Site Domain" spam submissions are restored /
//   reprocessed (after adding ledgertc.com to Advanced Tracking), HubSpot
//   re-runs its analytics engine over the original form event. That can flip
//   carefully API-set attribution (hs_analytics_source = PAID_SEARCH) back to
//   HubSpot's inferred value. This script lets you check whether the current
//   winning value of each at-risk property is still API (safe) or has been
//   clobbered to FORM/ANALYTICS (bad).
//
// Usage:
//   node --env-file=.env scripts/snapshot-attribution.js 225589106029
//   node --env-file=.env scripts/snapshot-attribution.js --email someone@example.com
//   node --env-file=.env scripts/snapshot-attribution.js 225589106029 --json > before.json
//   # then, after a restore, run again and diff:
//   node --env-file=.env scripts/snapshot-attribution.js 225589106029 --json > after.json
//
// Env vars:
//   HUBSPOT_TOKEN      — required. Private app token (Contacts read scope)
//   HUBSPOT_PORTAL_ID  — optional. For a clickable contact URL in the output

const HUBSPOT_API = "https://api.hubapi.com";

// Properties whose "winner source" we care about. The first group is pure
// attribution (API-set by submit-form.js, the thing a restore can clobber);
// the second group is form-mapped contact data that a restore could also
// overwrite if the API had later written a cleaned value.
const ATTRIBUTION_PROPS = [
  "hs_analytics_source",
  "hs_analytics_source_data_1",
  "hs_analytics_source_data_2",
  "hs_google_click_id",
  "gad_campaignid",
];
const FORM_MAPPED_PROPS = [
  "firstname",
  "lastname",
  "phone",
  "company",
  "form_source",
];
const ALL_PROPS = [...ATTRIBUTION_PROPS, ...FORM_MAPPED_PROPS, "email", "createdate"];

// Source types HubSpot stamps on each historical value. We flag which ones are
// "good" (attribution we set / safe) vs "bad" (HubSpot's engine won) for the
// attribution properties specifically.
const GOOD_SOURCES = new Set(["API", "INTEGRATION", "IMPORT", "MIGRATION"]);
const BAD_SOURCES = new Set(["ANALYTICS", "FORM", "CONTACTS_WEB"]);

function parseArgs(argv) {
  const args = { id: null, email: null, json: false };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--email") args.email = argv[++i];
    else if (a === "--id") args.id = argv[++i];
    else if (a === "--json") args.json = true;
    else if (a === "--help" || a === "-h") {
      console.log("Usage: node --env-file=.env scripts/snapshot-attribution.js <contactId | --email x@y.com> [--json]");
      process.exit(0);
    } else if (!a.startsWith("--")) {
      // Positional: numeric → id, contains @ → email
      if (/^\d+$/.test(a)) args.id = a;
      else if (a.includes("@")) args.email = a;
    }
  }
  return args;
}

async function hubspot(method, path, body) {
  const token = process.env.HUBSPOT_TOKEN;
  if (!token) throw new Error("HUBSPOT_TOKEN env var is required");
  const opts = {
    method,
    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
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

async function resolveContactId(args) {
  if (args.id) return args.id;
  if (!args.email) throw new Error("Provide a contact id or --email");
  const r = await hubspot("POST", "/crm/v3/objects/contacts/search", {
    filterGroups: [{ filters: [{ propertyName: "email", operator: "EQ", value: args.email.toLowerCase() }] }],
    properties: ["email"],
    limit: 1,
  });
  const c = r.results && r.results[0];
  if (!c) throw new Error(`No contact found for email ${args.email}`);
  return c.id;
}

async function fetchContactWithHistory(id) {
  const qs = new URLSearchParams();
  qs.set("propertiesWithHistory", ALL_PROPS.join(","));
  qs.set("properties", ALL_PROPS.join(","));
  return hubspot("GET", `/crm/v3/objects/contacts/${id}?${qs.toString()}`);
}

// HubSpot returns history newest-first; entry[0] is the current winner.
function describeHistory(history) {
  if (!history || !history.length) return { current: null, source: null, entries: [] };
  const entries = history.map(h => ({
    value: h.value,
    source: h.sourceType || h.source || "?",
    sourceId: h.sourceId || "",
    timestamp: h.timestamp || "",
  }));
  return { current: entries[0].value, source: entries[0].source, entries };
}

function flagFor(prop, source) {
  if (!ATTRIBUTION_PROPS.includes(prop)) return "";          // only judge attribution props
  if (source && GOOD_SOURCES.has(source)) return "  ✅ API-set (safe)";
  if (source && BAD_SOURCES.has(source)) return "  ⚠️  HubSpot engine won (attribution clobbered)";
  return "";
}

function printReport(contact, contactUrl) {
  const pwh = contact.propertiesWithHistory || {};
  const p = contact.properties || {};

  console.log("");
  console.log(`Contact ${contact.id}  ${p.email || "(no email)"}`);
  if (contactUrl) console.log(contactUrl);
  console.log(`created: ${p.createdate || "?"}`);
  console.log("─".repeat(72));

  const section = (title, props) => {
    console.log(`\n${title}`);
    for (const prop of props) {
      const h = describeHistory(pwh[prop]);
      const val = h.current === null || h.current === undefined || h.current === "" ? "(empty)" : h.current;
      console.log(`  ${prop}`);
      console.log(`    = ${val}   [winner source: ${h.source || "—"}]${flagFor(prop, h.source)}`);
      // Show prior values so a clobber is visible (what it used to be)
      if (h.entries.length > 1) {
        for (const e of h.entries.slice(1, 4)) {
          const ev = e.value === "" ? "(empty)" : e.value;
          console.log(`      ↳ was: ${ev}   [${e.source}]  ${e.timestamp}`);
        }
        if (h.entries.length > 4) console.log(`      … ${h.entries.length - 4} older value(s)`);
      }
    }
  };

  section("ATTRIBUTION (API-set by submit-form.js — at risk from restore):", ATTRIBUTION_PROPS);
  section("FORM-MAPPED (could be overwritten if API wrote a cleaned value):", FORM_MAPPED_PROPS);

  // Verdict line for the headline property
  const src = describeHistory(pwh["hs_analytics_source"]).source;
  console.log("\n" + "─".repeat(72));
  if (GOOD_SOURCES.has(src)) {
    console.log(`VERDICT: hs_analytics_source winner is ${src} → attribution INTACT ✅`);
  } else if (BAD_SOURCES.has(src)) {
    console.log(`VERDICT: hs_analytics_source winner is ${src} → attribution CLOBBERED ⚠️  (re-assert via API)`);
  } else {
    console.log(`VERDICT: hs_analytics_source winner source = ${src || "none"} (inspect manually)`);
  }
  console.log("");
}

async function main() {
  const args = parseArgs(process.argv);
  const id = await resolveContactId(args);
  const contact = await fetchContactWithHistory(id);
  const portal = process.env.HUBSPOT_PORTAL_ID;
  const contactUrl = portal ? `https://app.hubspot.com/contacts/${portal}/contact/${id}` : "";

  if (args.json) {
    // Machine-readable: prop → { current, source, entries[] }, for before/after diffing
    const pwh = contact.propertiesWithHistory || {};
    const out = { id: contact.id, email: (contact.properties || {}).email || null, properties: {} };
    for (const prop of ALL_PROPS) out.properties[prop] = describeHistory(pwh[prop]);
    console.log(JSON.stringify(out, null, 2));
  } else {
    printReport(contact, contactUrl);
  }
}

main().catch(err => {
  console.error(`\n✖ ${err.message}\n`);
  process.exit(1);
});
