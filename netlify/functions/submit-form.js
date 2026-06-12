// Ledger Trade & Capital - Contact Form Handler
// Netlify Function: /.netlify/functions/submit-form
//
// This function receives form submissions from the website contact page
// and processes them through HubSpot:
//   1. Look up or create Contact (de-dupe by email)
//   2. Look up or create Company (de-dupe by name)
//   3. Associate Contact <-> Company (only if Company is new)
//   4. Determine Ticket owner (Contact owner > Company owner > Russell)
//   5. Create Ticket with full submission summary
//   6. Associate Ticket to Contact and Company
//
// Environment variables required (set in Netlify dashboard):
//   HUBSPOT_TOKEN           - Private app token
//   HUBSPOT_PORTAL_ID       - HubSpot portal/account ID
//   HUBSPOT_PIPELINE_ID     - "Inbound Inquiries" pipeline ID
//   HUBSPOT_DEFAULT_OWNER_EMAIL - Fallback owner (russell@ledgertc.com)
//   FB_CAPI_TOKEN           - Meta Conversions API system user token (optional)

const crypto = require("crypto");
const HUBSPOT_API = "https://api.hubapi.com";
const FB_PIXEL_ID = "1982014519404337";
const FB_CAPI_URL = `https://graph.facebook.com/v19.0/${FB_PIXEL_ID}/events`;

// ─── Campaign attribution: map form_source to clean campaign name ──
// Hard-channel forms always map to the same campaign regardless of traffic source.
const FORM_SOURCE_TO_CAMPAIGN = {
  "construction-landing-page-google-ads": "GUC",
  "first-time-builder-landing-page-google-ads": "GUC",
  "first-time-builder-landing-page": "GUC",
  "dscr-landing-page-google-ads": "DSCR",
  "fix-and-flip-landing-page-google-ads": "FnF",
  "broker-partner": "Broker",
  "rtl-calculator": "RTL Calculator",
  "dscr-calculator": "DSCR Calculator",
};

// Pages that can be hit paid OR organic — attribution depends on traffic source.
// Paid -> "Conquest" for competitor pages, or the real campaign via utm_campaign
// for get-in-touch (which is in site nav and reachable from any ad LP).
const COMPETITOR_FORM_SOURCES = new Set([
  "ledger-vs-kiavi-lp",
  "ledger-vs-civic-financial-services-lp",
  "ledger-vs-lima-one-capital-lp",
  "ledger-vs-renovo-financial-lp",
  "ledger-vs-conventus-lp",
  "ledger-vs-cofi-lending-lp",
  "ledger-vs-housemax-funding-lp",
  "ledger-vs-groundfloor-lp",
  "ledger-vs-easy-street-capital-lp",
  "ledger-vs-temple-view-capital-lp",
  "compare-lenders-landing-page",
]);

// Map ads LP sessionStorage flag to campaign (fallback when gclid stripped)
const ADS_LP_TO_CAMPAIGN = {
  "fix-and-flip-lp": "FnF",
  "construction-loans-lp": "GUC",
  "dscr-lp": "DSCR",
};

function deriveAdCampaign(formSource, utmCampaign, adsLp, isGoogleAds) {
  // 1) Hard-channel forms
  if (formSource && FORM_SOURCE_TO_CAMPAIGN[formSource]) {
    return FORM_SOURCE_TO_CAMPAIGN[formSource];
  }
  // 2) Competitor conquest pages: paid -> Conquest, organic -> Website
  if (COMPETITOR_FORM_SOURCES.has(formSource)) {
    return isGoogleAds ? "Conquest" : "Website";
  }
  // 3) Vertical construction product page (organic product content)
  if (formSource === "construction-loan") {
    return isGoogleAds ? (utmCampaign || "GUC") : "Website";
  }
  // 4) Get in Touch: accessible from site nav, so a paid ad click can land here
  //    after bouncing around. Fall through to utm_campaign/adsLp when paid,
  //    "Website" when organic.
  if (formSource === "get-in-touch") {
    if (isGoogleAds) {
      if (utmCampaign) return utmCampaign;
      if (adsLp && ADS_LP_TO_CAMPAIGN[adsLp]) return ADS_LP_TO_CAMPAIGN[adsLp];
      return "Paid Search";
    }
    return "Website";
  }
  // 5) Generic fallbacks for anything else
  if (utmCampaign) return utmCampaign;
  if (adsLp && ADS_LP_TO_CAMPAIGN[adsLp]) return ADS_LP_TO_CAMPAIGN[adsLp];
  return "";
}

// ─── Forms API: register timeline event for a submission ──────────
// Posts to HubSpot's form submission endpoint so "Submitted form: <name>"
// appears on the contact's Activities timeline. Does NOT create a contact —
// our own findOrCreateContact has already done that with full attribution.
// Configured with createNewContactForNewEmail=false on the form side, so a
// missing match is a no-op rather than a phantom contact.
//
// Form GUIDs are created by scripts/create_hubspot_forms.py and baked in.
// Shared GUIDs: all 10 competitor pages roll up to compare_lenders; the two
// FTB form_source variants (paid vs organic) roll up to first_time_builder.
const HUBSPOT_PORTAL_ID = "46107229";
const FORM_GUIDS = {
  "get-in-touch":                              "453a5590-5369-4df7-ae57-6e7c3177264e",
  "broker-partner":                            "6a45af48-942c-4ac5-8c32-e73b8375cd33",
  "construction-landing-page-google-ads":      "e1827aa1-13a8-400d-9012-29fe493596f1",
  "dscr-landing-page-google-ads":              "c02a39cf-158a-48c7-95a5-8fa05176ddfc",
  "fix-and-flip-landing-page-google-ads":      "6099f525-8886-485f-a5cb-261677d3538e",
  "first-time-builder-landing-page":           "2f55435d-b2e1-475d-b621-4cc74e1b3c8f",
  "first-time-builder-landing-page-google-ads":"2f55435d-b2e1-475d-b621-4cc74e1b3c8f",
  "construction-loan":                         "27670eb0-f770-46e8-8d9a-adede5e7911e",
  "rtl-calculator":                            "88870c3a-2936-4071-8666-4f8bb3c8ad72",
  "dscr-calculator":                           "d8d8fa08-3e9d-4716-829b-bb6f4d73ea20",
  "compare-lenders-landing-page":              "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-kiavi-lp":                        "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-civic-financial-services-lp":     "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-lima-one-capital-lp":             "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-renovo-financial-lp":             "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-conventus-lp":                    "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-cofi-lending-lp":                 "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-housemax-funding-lp":             "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-groundfloor-lp":                  "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-easy-street-capital-lp":          "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
  "ledger-vs-temple-view-capital-lp":          "596223ef-cc8c-4d71-a7ae-7cc87f4d5e40",
};

async function registerFormSubmission(formData) {
  const guid = FORM_GUIDS[formData.formSource];
  if (!guid || !formData.email) return;
  const fields = [
    { objectTypeId: "0-1", name: "email",           value: formData.email },
    { objectTypeId: "0-1", name: "firstname",       value: formData.firstName || "" },
    { objectTypeId: "0-1", name: "lastname",        value: formData.lastName || "" },
    { objectTypeId: "0-1", name: "phone",           value: formData.phone || "" },
    { objectTypeId: "0-1", name: "form_source",     value: formData.formSource || "" },
    { objectTypeId: "0-1", name: "ad_campaign",     value: formData.adCampaign || "" },
    { objectTypeId: "0-1", name: "project_details", value: formData.projectDetails || "" },
  ];
  const context = {
    pageUri:  formData.pageUrl || "",
    pageName: formData.pageName || "",
  };
  if (formData.hutk) context.hutk = formData.hutk;
  try {
    const res = await fetch(
      `https://api.hsforms.com/submissions/v3/integration/submit/${HUBSPOT_PORTAL_ID}/${guid}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fields, context }),
      }
    );
    if (res.ok) {
      console.log(`Forms API event registered for ${formData.email} (form_source=${formData.formSource})`);
    } else {
      const text = await res.text();
      console.error(`Forms API submission failed [${res.status}] for ${formData.formSource}: ${text.slice(0, 300)}`);
    }
  } catch (err) {
    console.error("Forms API submission error:", err);
  }
}

// ─── Extract structured HubSpot fields from calculator JSON ──────
// DSCR calc → loan_type_interest = "DSCR Rental"
// RTL calc  → "Ground-Up Construction" (if projectType has "ground"/"new")
//             or "Fix & Flip / Bridge" (default)
function bucketLoanAmount(amt) {
  const n = Number(amt);
  if (!n || isNaN(n)) return "";
  if (n < 250000) return "Under $250K";
  if (n < 750000) return "$250K – $750K";
  if (n < 2000000) return "$750K – $2M";
  if (n < 5000000) return "$2M – $5M";
  return "$5M+";
}
function normalizePropertyType(t) {
  if (!t) return "";
  const s = String(t).toLowerCase();
  if (s.includes("sfr") || s.includes("single")) return "Single-Family / SFR";
  if (s.includes("2-4") || s.includes("2–4") || s.includes("duplex") || s.includes("triplex") || s.includes("fourplex")) return "2-4 Unit";
  if (s.includes("5+") || s.includes("multi")) return "5+ Unit / Small Multifamily";
  if (s.includes("townhome") || s.includes("condo")) return "Townhome / Condo";
  if (s.includes("land") || s.includes("ground")) return "Land + Build (Ground-Up)";
  return "";
}
function extractCalculatorFields(formSource, jsonStr) {
  if (!jsonStr) return {};
  let d;
  try { d = JSON.parse(jsonStr); } catch { return {}; }
  const out = {};
  if (formSource === "dscr-calculator") {
    out.loanTypeInterest = "DSCR Rental";
  } else if (formSource === "rtl-calculator") {
    const proj = String(d.projectType || d.dealType || "").toLowerCase();
    out.loanTypeInterest = (proj.includes("ground") || proj.includes("new construction") || proj.includes("spec"))
      ? "Ground-Up Construction"
      : "Fix & Flip / Bridge";
  }
  const stateCandidate = d.stateName || d.state || "";
  if (stateCandidate) out.propertyState = stateCandidate;
  const amtCandidate = d.loanAmt || d.tla || d.day1 || 0;
  const bucket = bucketLoanAmount(amtCandidate);
  if (bucket) out.loanAmountRange = bucket;
  const pt = normalizePropertyType(d.propertyType || d.assetType || "");
  if (pt) out.propertyTypeInterest = pt;
  return out;
}

// ─── Format calculator results JSON into a readable one-liner ────
function formatCalculatorResults(jsonStr) {
  if (!jsonStr) return "";
  try {
    const d = JSON.parse(jsonStr);
    const parts = [];
    // DSCR calculator fields
    if (d.program) parts.push(d.program);
    if (d.loanAmt) parts.push("Loan $" + Number(d.loanAmt).toLocaleString());
    if (d.propertyValue) parts.push("Property $" + Number(d.propertyValue).toLocaleString());
    if (d.ltv) parts.push("LTV " + d.ltv);
    if (d.dscr) parts.push("DSCR " + d.dscr);
    if (d.rate) parts.push("Rate " + d.rate + "%");
    if (d.monthlyPayment) parts.push("PITIA $" + Number(d.monthlyPayment).toLocaleString());
    if (d.loanPurpose) parts.push(d.loanPurpose);
    if (d.io) parts.push("Interest-Only");
    if (d.propertyType) parts.push(d.propertyType);
    if (d.stateName) parts.push(d.stateName);
    // RTL/construction calculator fields
    if (d.tla) parts.push("TLA $" + Number(d.tla).toLocaleString());
    if (d.day1) parts.push("Day1 $" + Number(d.day1).toLocaleString());
    if (d.holdback) parts.push("Holdback $" + Number(d.holdback).toLocaleString());
    if (d.ltc) parts.push("LTC " + d.ltc);
    if (d.ltaiv) parts.push("LTAIV " + d.ltaiv);
    if (d.purchasePrice) parts.push("Purchase $" + Number(d.purchasePrice).toLocaleString());
    if (d.rehabBudget) parts.push("Rehab $" + Number(d.rehabBudget).toLocaleString());
    if (d.arv) parts.push("ARV $" + Number(d.arv).toLocaleString());
    if (d.asIsValue) parts.push("As-Is $" + Number(d.asIsValue).toLocaleString());
    if (d.fico) parts.push("FICO " + d.fico);
    if (d.experience !== undefined && d.experience !== "") parts.push("Exp " + d.experience);
    if (d.assetType) parts.push(d.assetType);
    if (d.dealType) parts.push(d.dealType);
    if (d.loanTerm) parts.push(d.loanTerm + "mo");
    if (d.tier) parts.push("Tier " + d.tier);
    return parts.join(" | ");
  } catch (e) {
    return jsonStr;
  }
}

// ─── Disposable / throwaway email domains (silent-reject on match) ──
const DISPOSABLE_EMAIL_DOMAINS = new Set([
  // High-volume throwaway services
  "mailinator.com", "guerrillamail.com", "guerrillamail.de", "guerrillamail.net",
  "guerrillamail.org", "guerrillamailblock.com", "grr.la", "sharklasers.com",
  "guerrillamail.info", "tempmail.com", "temp-mail.org", "temp-mail.io",
  "throwaway.email", "throwaway.cc", "throwamail.com",
  "yopmail.com", "yopmail.fr", "yopmail.net", "yopmail.gq",
  "trashmail.com", "trashmail.me", "trashmail.net", "trashmail.org",
  "dispostable.com", "mailnesia.com", "maildrop.cc",
  "fakeinbox.com", "fakemail.net", "10minutemail.com", "10minutemail.net",
  "20minutemail.com", "tempail.com", "tempr.email",
  "discard.email", "discardmail.com", "discardmail.de",
  "mailcatch.com", "mailexpire.com", "mailnull.com",
  "mailinater.com", "mailforspam.com",
  "getnada.com", "nada.email", "anonbox.net",
  "mytemp.email", "mohmal.com", "emailondeck.com",
  "mintemail.com", "tempinbox.com", "harakirimail.com",
  "mailsac.com", "inboxkitten.com", "burnermail.io",
  "crazymailing.com", "armyspy.com", "dayrep.com",
  "einrot.com", "fleckens.hu", "gustr.com", "jourrapide.com",
  "rhyta.com", "superrito.com", "teleworm.us",
  // Russian / international spam favorites
  "mail.ru", "bk.ru", "list.ru", "inbox.ru",
  "rambler.ru", "autorambler.ru", "myrambler.ru",
  "ro.ru", "front.ru", "hotbox.ru",
  // Other commonly abused
  "sharklasers.com", "spam4.me", "spamgourmet.com",
  "trashymail.com", "uggsrock.com", "wegwerfmail.de",
  "zoemail.org", "mailzilla.com", "spamfree24.org",
  "objectmail.com",
  "emltmp.com", "tmpmail.net", "tmpmail.org",
  "guerrillamailblock.com", "clrmail.com",
  "mailtemp.net", "emailfake.com", "tempmailo.com",
  "tempmailaddress.com", "tmails.net",
]);

// ─── Rate-limiting & daily-cap stores (in-memory, per instance) ──
const ipSubmissions = new Map();   // ip -> [timestamp, …]
const IP_MAX = 5;
const IP_WINDOW_MS = 60 * 60 * 1000;  // 1 hour

let dailyCount = 0;
let dailyResetDate = new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
const DAILY_CAP = 100;

function pruneAndCount(ip) {
  const now = Date.now();
  const cutoff = now - IP_WINDOW_MS;
  const timestamps = (ipSubmissions.get(ip) || []).filter((t) => t > cutoff);
  ipSubmissions.set(ip, timestamps);
  return timestamps.length;
}

function recordIp(ip) {
  const timestamps = ipSubmissions.get(ip) || [];
  timestamps.push(Date.now());
  ipSubmissions.set(ip, timestamps);
}

function checkAndIncrementDaily() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== dailyResetDate) {
    dailyCount = 0;
    dailyResetDate = today;
  }
  dailyCount++;
  return dailyCount;
}

async function sendDailyCapAlert() {
  try {
    await fetch("https://api.mailchannels.net/tx/v1/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: "Info@ledgertc.com" }] }],
        from: { email: "noreply@ledgertc.com", name: "Ledger TC Website" },
        subject: "Daily form submission cap reached (100)",
        content: [{
          type: "text/plain",
          value: `The contact form on ledgertc.com has received ${DAILY_CAP} submissions today (${new Date().toISOString().slice(0, 10)}). Further submissions are being rejected until midnight UTC. This may indicate a spam attack — please review recent HubSpot tickets.`,
        }],
      }),
    });
    console.log("Daily cap alert email sent to Info@ledgertc.com");
  } catch (err) {
    console.error("Failed to send daily cap alert email:", err);
  }
}

// ─── Per-attempt submission logger (Slack → #nervo_raw_subs) ──────
// Posts a single structured line to a muted Slack channel for every
// submission attempt that reaches the handler, regardless of outcome.
// scripts/audit-submissions.js parses these messages weekly to compute
// "attempts → contacts → tickets" funnel. Each message is:
//   submission_attempt {"outcome":"...","formSource":"...","email":"..."}
// Fire-and-forget — wrapped in catch() so a Slack outage never affects
// the user response.
async function logRawSubmission(outcome, ctx) {
  const url = process.env.SLACK_RAW_WEBHOOK_URL;
  if (!url) return;
  try {
    const payload = {
      outcome,
      formSource: (ctx && ctx.formSource) || "-",
      email: (ctx && ctx.email) || "-",
    };
    if (ctx && ctx.err) payload.err = String(ctx.err).slice(0, 200);
    // Errors carry the full lead identity + message so a dropped submission
    // can be reconstructed from this channel alone (the 6/10 INVALID_EMAIL
    // drop lost the phone/message because only the email was logged).
    if (outcome === "error") {
      if (ctx && ctx.name) payload.name = ctx.name;
      if (ctx && ctx.phone) payload.phone = ctx.phone;
      if (ctx && ctx.projectOverview) payload.details = String(ctx.projectOverview).slice(0, 500);
    }
    await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text: `submission_attempt ${JSON.stringify(payload)}` }),
    });
  } catch (err) {
    console.warn("Raw submission log failed:", err.message);
  }
}

// ─── Helper: make HubSpot API request ─────────────────────────────
async function hubspot(method, path, body) {
  const opts = {
    method,
    headers: {
      "Authorization": `Bearer ${process.env.HUBSPOT_TOKEN}`,
      "Content-Type": "application/json",
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(`${HUBSPOT_API}${path}`, opts);
  const text = await res.text();

  let data;
  try { data = JSON.parse(text); } catch { data = text; }

  if (!res.ok) {
    console.error(`HubSpot API error [${res.status}] ${method} ${path}:`, data);
    return { error: true, status: res.status, data };
  }
  return data;
}

// ─── Analytics: Associate hutk with contact ───────────────────────
async function associateHutkWithContact(email, hutk, pageUri, pageName) {
  if (!hutk || !email) return;
  try {
    const res = await fetch(`${HUBSPOT_API}/contacts/v1/contact/createOrUpdate/email/${encodeURIComponent(email)}`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.HUBSPOT_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        properties: [],
        context: {
          hutk,
          pageUri: pageUri || "",
          pageName: pageName || "",
        },
      }),
    });
    const text = await res.text();
    console.log(`hutk associated for ${email}: status ${res.status}`);
  } catch (err) {
    console.error("Failed to associate hutk:", err);
  }
}

// ─── Re-assert tracking after hutk association ───────────────────
// The hutk association call (v1 createOrUpdate) triggers HubSpot's analytics
// engine which can overwrite hs_analytics_source. This PATCH runs after hutk
// to force our Google Ads attribution back onto the contact.
async function ensureTrackingAfterHutk(contactId, formData) {
  if (!formData.isGoogleAds) return;

  const props = {
    hs_analytics_source: "PAID_SEARCH",
  };
  if (formData.googleClickId) {
    props.hs_google_click_id = formData.googleClickId;
  }
  if (formData.gadCampaignId) {
    props.gad_campaignid = formData.gadCampaignId;
  }

  const result = await hubspot("PATCH", `/crm/v3/objects/contacts/${contactId}`, {
    properties: props,
  });
  if (!result.error) {
    console.log(`Re-asserted tracking on contact ${contactId} after hutk`);
  } else {
    console.error(`Failed to re-assert tracking on ${contactId}:`, result.data);
  }
}

// ─── Step 1: Contact lookup / creation ────────────────────────────
async function findOrCreateContact(formData) {
  // Search by email (preferred) or phone (fallback for calc forms with no email)
  const searchProps = ["email", "firstname", "lastname", "hubspot_owner_id", "hs_google_click_id", "gad_campaignid", "source_website", "form_source", "ad_campaign", "utm_campaign", "hs_analytics_source", "hs_analytics_source_data_1", "project_details"];
  let search = { total: 0 };
  if (formData.email) {
    search = await hubspot("POST", "/crm/v3/objects/contacts/search", {
      filterGroups: [{ filters: [{ propertyName: "email", operator: "EQ", value: formData.email }] }],
      properties: searchProps,
    });
  } else if (formData.phone) {
    search = await hubspot("POST", "/crm/v3/objects/contacts/search", {
      filterGroups: [{ filters: [{ propertyName: "phone", operator: "EQ", value: formData.phone }] }],
      properties: searchProps,
    });
  }

  if (search.total > 0) {
    const existing = search.results[0];
    console.log(`Contact found: ${existing.id} (${formData.email || formData.phone})`);
    await backfillTracking(existing, formData);
    return { contact: existing, isNew: false };
  }

  // Create new contact
  const contactProps = {
    firstname: formData.firstName,
    lastname: formData.lastName,
    lifecyclestage: "lead",
    source_website: "Yes",
    ...(formData.email && { email: formData.email }),
    ...(formData.phone && { phone: formData.phone }),
    ...(formData.googleClickId && { hs_google_click_id: formData.googleClickId }),
    ...(formData.gadCampaignId && { gad_campaignid: formData.gadCampaignId }),
    ...(formData.formSource && { form_source: formData.formSource }),
    ...(formData.adCampaign && { ad_campaign: formData.adCampaign }),
    ...(formData.utmCampaign && { utm_campaign: formData.utmCampaign }),
    ...(formData.projectDetails && { project_details: formData.projectDetails }),
    ...(formData.message && { message: formData.message }),
  };

  // Set source attribution
  if (formData.isGoogleAds) {
    contactProps.hs_analytics_source = "PAID_SEARCH";
  } else {
    // Fallback for when the HubSpot tracking pixel is blocked (corporate
    // networks, ad blockers, privacy browsers). When the pixel works, the
    // hutk association re-attributes to the true session source. This initial
    // value only sticks when hutk is absent. Without it, HS would otherwise
    // stamp the contact OFFLINE / INTEGRATION because we create via CRM API.
    // (hs_analytics_source_data_1 is read-only via the API, so we can't
    // overwrite the INTEGRATION stamp; the page URL is captured in
    // hs_analytics_first_url and form_source for reporting.)
    contactProps.hs_analytics_source = "DIRECT_TRAFFIC";
  }

  const created = await hubspot("POST", "/crm/v3/objects/contacts", {
    properties: contactProps,
  });

  if (created.error) {
    // Race condition: HubSpot collected forms may have created the contact
    // between our search and create. Search again and backfill.
    if (created.status === 409) {
      console.log(`Contact create conflict for ${formData.email || formData.phone} — retrying search`);
      const retryProp = formData.email ? "email" : "phone";
      const retryVal = formData.email || formData.phone;
      const retry = await hubspot("POST", "/crm/v3/objects/contacts/search", {
        filterGroups: [{
          filters: [{ propertyName: retryProp, operator: "EQ", value: retryVal }]
        }],
        properties: ["email", "firstname", "lastname", "hubspot_owner_id", "hs_google_click_id", "gad_campaignid", "source_website", "form_source", "ad_campaign", "utm_campaign", "hs_analytics_source", "hs_analytics_source_data_1", "project_details"],
      });
      if (retry.total > 0) {
        const existing = retry.results[0];
        console.log(`Contact found on retry: ${existing.id} (${formData.email || formData.phone})`);
        await backfillTracking(existing, formData);
        return { contact: existing, isNew: false };
      }
    }

    // HubSpot rejected the email (typo like "gmail.comg" passes our regex but
    // fails HubSpot's stricter validation). Salvage the lead as a phone-only
    // contact — same shape as calculator submissions — with the rejected email
    // noted in project_details so the team can correct it manually.
    if (created.status === 400 && JSON.stringify(created.data).includes("INVALID_EMAIL") && formData.phone) {
      console.warn(`HubSpot rejected email "${formData.email}" as invalid — retrying as phone-only contact`);
      const phoneSearch = await hubspot("POST", "/crm/v3/objects/contacts/search", {
        filterGroups: [{ filters: [{ propertyName: "phone", operator: "EQ", value: formData.phone }] }],
        properties: searchProps,
      });
      const badEmailNote = `[Submitted email rejected by HubSpot as invalid: ${formData.email}]`;
      if (phoneSearch.total > 0) {
        const existing = phoneSearch.results[0];
        console.log(`Contact found by phone after invalid email: ${existing.id} (${formData.phone})`);
        formData.invalidEmail = formData.email;
        formData.email = "";
        await backfillTracking(existing, formData);
        return { contact: existing, isNew: false };
      }
      const retryProps = { ...contactProps };
      delete retryProps.email;
      retryProps.project_details = retryProps.project_details
        ? `${retryProps.project_details} | ${badEmailNote}`
        : badEmailNote;
      const retried = await hubspot("POST", "/crm/v3/objects/contacts", { properties: retryProps });
      if (!retried.error) {
        console.log(`Contact created phone-only after invalid email: ${retried.id} (${formData.phone})`);
        // Blank the email so downstream email-keyed calls (hutk association,
        // Forms API, contact search on resubmit) don't re-send the bad address.
        formData.invalidEmail = formData.email;
        formData.email = "";
        return { contact: retried, isNew: true };
      }
      console.error(`Phone-only retry also failed for ${formData.phone}:`, retried.data);
    }

    throw new Error(`Failed to create contact: ${JSON.stringify(created.data)}`);
  }

  console.log(`Contact created: ${created.id} (${formData.email || formData.phone})`);
  return { contact: created, isNew: true };
}

// Backfill tracking properties on existing contacts (e.g. created by HubSpot collected forms)
async function backfillTracking(existing, formData) {
  const updateProps = {};
  if (formData.googleClickId && !existing.properties.hs_google_click_id) {
    updateProps.hs_google_click_id = formData.googleClickId;
  }
  if (formData.gadCampaignId && !existing.properties.gad_campaignid) {
    updateProps.gad_campaignid = formData.gadCampaignId;
  }
  if (!existing.properties.source_website) {
    updateProps.source_website = "Yes";
  }
  // Calculator submissions upgrade the label over prior LP submissions
  // (higher-intent signal). Non-calculator submissions preserve existing values.
  const isCalcSubmit = formData.formSource === "rtl-calculator" || formData.formSource === "dscr-calculator";
  if (formData.formSource && (!existing.properties.form_source || isCalcSubmit)) {
    updateProps.form_source = formData.formSource;
  }
  if (formData.adCampaign && (!existing.properties.ad_campaign || isCalcSubmit)) {
    updateProps.ad_campaign = formData.adCampaign;
  }
  if (formData.utmCampaign && !existing.properties.utm_campaign) {
    updateProps.utm_campaign = formData.utmCampaign;
  }
  if (formData.isGoogleAds) {
    updateProps.hs_analytics_source = "PAID_SEARCH";
  } else if (existing.properties.hs_analytics_source_data_1 === "INTEGRATION") {
    // Previously stamped OFFLINE / INTEGRATION because hutk was missing on the
    // first submit (blocked pixel). Repair the source on resubmit.
    // (source_data_1 itself is read-only via the API and will stay "INTEGRATION"
    // on repaired contacts, but the headline source is what drives reporting.)
    updateProps.hs_analytics_source = "DIRECT_TRAFFIC";
  }
  // Preserve user-typed project overview text. Calculator submissions send only
  // a calc summary (no textarea); without this guard, a calc submit after an LP
  // submit would overwrite the typed text. Only update project_details when the
  // new submission has typed text, or the existing field is empty.
  const hasTypedText = !!formData.projectOverview;
  const existingDetails = existing.properties.project_details || "";
  if (formData.projectDetails && (hasTypedText || !existingDetails)) {
    updateProps.project_details = formData.projectDetails;
  }
  if (Object.keys(updateProps).length > 0) {
    const updated = await hubspot("PATCH", `/crm/v3/objects/contacts/${existing.id}`, {
      properties: updateProps,
    });
    if (!updated.error) {
      console.log(`Backfilled tracking on contact ${existing.id}:`, Object.keys(updateProps).join(', '));
    } else {
      console.error(`Failed to backfill tracking on ${existing.id}:`, updated.data);
    }
  }
}

// ─── Secondary PATCH: new structured form fields ─────────────────
// These properties may not exist yet in HubSpot (scope-gated creation).
// Wrapped individually so a single missing property doesn't drop the rest.
async function patchContactWithFormFields(contactId, formData) {
  const candidates = {
    loan_type_of_interest: formData.loanTypeInterest,
    property_type_of_interest: formData.propertyTypeInterest,
    property_state: formData.propertyState,
    loan_amount_range: formData.loanAmountRange,
    deal_timeline: formData.dealTimeline,
    construction_type: formData.constructionType,
  };
  const props = Object.fromEntries(
    Object.entries(candidates).filter(([_, v]) => v && String(v).trim() !== "")
  );
  if (Object.keys(props).length === 0) return;

  const result = await hubspot("PATCH", `/crm/v3/objects/contacts/${contactId}`, { properties: props });
  if (result.error) {
    // If bulk PATCH fails (likely property doesn't exist), retry one at a time
    // so the remaining known-good properties still land.
    console.warn(`Bulk form-field PATCH failed on contact ${contactId}; retrying per-property`);
    for (const [name, value] of Object.entries(props)) {
      const one = await hubspot("PATCH", `/crm/v3/objects/contacts/${contactId}`, {
        properties: { [name]: value },
      });
      if (one.error) {
        console.warn(`Skipped form-field ${name} on contact ${contactId} (likely property not yet created in HubSpot)`);
      }
    }
  } else {
    console.log(`Patched form fields on contact ${contactId}: ${Object.keys(props).join(", ")}`);
  }
}

// ─── Step 2: Company lookup / creation ────────────────────────────
function activityToConnectValue(formData) {
  return formData.isGoogleAds ? "Ads" : "Website";
}

const BEFORE_CONNECTED_STAGES = ["946683714", "lead"];

async function maybeUpdateActivityToConnect(companyId, company, formData) {
  const stage = (company.properties && company.properties.lifecyclestage) || "";
  if (BEFORE_CONNECTED_STAGES.includes(stage) || !stage) {
    const newValue = activityToConnectValue(formData);
    await hubspot("PATCH", `/crm/v3/objects/companies/${companyId}`, {
      properties: { activity_to_connect: newValue },
    });
    console.log(`Updated activity_to_connect to "${newValue}" for company ${companyId}`);
  } else {
    console.log(`Company ${companyId} already connected (stage: ${stage}), skipping activity_to_connect update`);
  }
}

async function findOrCreateCompany(companyName, formData) {
  if (!companyName || companyName.trim() === "") {
    return { company: null, isNew: false };
  }

  // Search by name
  const search = await hubspot("POST", "/crm/v3/objects/companies/search", {
    filterGroups: [{
      filters: [{ propertyName: "name", operator: "EQ", value: companyName.trim() }]
    }],
    properties: ["name", "coverage", "lifecyclestage", "activity_to_connect"],
  });

  if (search.total > 0) {
    const existing = search.results[0];
    console.log(`Company found: ${existing.id} (${companyName})`);
    await maybeUpdateActivityToConnect(existing.id, existing, formData);
    return { company: existing, isNew: false };
  }

  // Create new company
  const created = await hubspot("POST", "/crm/v3/objects/companies", {
    properties: {
      name: companyName.trim(),
      source_website: "Yes",
      activity_to_connect: activityToConnectValue(formData),
    },
  });

  if (created.error) {
    throw new Error(`Failed to create company: ${JSON.stringify(created.data)}`);
  }

  console.log(`Company created: ${created.id} (${companyName})`);
  return { company: created, isNew: true };
}

// ─── Step 3: Associate Contact <-> Company ────────────────────────
async function associateContactToCompany(contactId, companyId) {
  const result = await hubspot(
    "PUT",
    `/crm/v4/objects/contacts/${contactId}/associations/companies/${companyId}`,
    [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 1 }]
  );
  if (result.error) {
    console.error("Failed to associate contact to company:", result.data);
  } else {
    console.log(`Associated contact ${contactId} <-> company ${companyId}`);
  }
}

// ─── Step 3b: Get existing company associated with a contact ─────
async function getContactCompany(contactId, formData) {
  const assoc = await hubspot(
    "GET",
    `/crm/v4/objects/contacts/${contactId}/associations/companies`
  );

  if (assoc.error || !assoc.results || assoc.results.length === 0) {
    return null;
  }

  const companyId = assoc.results[0].toObjectId;
  const company = await hubspot(
    "GET",
    `/crm/v3/objects/companies/${companyId}?properties=name,coverage,lifecyclestage,activity_to_connect`
  );

  if (company.error) {
    return null;
  }

  console.log(`Found existing associated company: ${company.id} (${company.properties.name})`);
  await maybeUpdateActivityToConnect(company.id, company, formData);
  return { company, isNew: false };
}

// ─── Step 4: Determine Ticket owner ──────────────────────────────
async function determineTicketOwner(contactResult, companyResult) {
  // Priority 1: Existing contact's owner
  if (!contactResult.isNew && contactResult.contact.properties.hubspot_owner_id) {
    console.log(`Ticket owner from existing contact: ${contactResult.contact.properties.hubspot_owner_id}`);
    return contactResult.contact.properties.hubspot_owner_id;
  }

  // Priority 2: Existing company's owner (stored in "coverage" property)
  if (companyResult.company && !companyResult.isNew && companyResult.company.properties.coverage) {
    console.log(`Ticket owner from existing company coverage: ${companyResult.company.properties.coverage}`);
    return companyResult.company.properties.coverage;
  }

  // Priority 3: Default owner (Russell)
  const defaultEmail = process.env.HUBSPOT_DEFAULT_OWNER_EMAIL || "russell@ledgertc.com";
  const owners = await hubspot("GET", `/crm/v3/owners?email=${encodeURIComponent(defaultEmail)}`);

  if (owners.results && owners.results.length > 0) {
    console.log(`Ticket owner defaulting to ${defaultEmail}: ${owners.results[0].id}`);
    return owners.results[0].id;
  }

  console.warn("Could not find default owner. Ticket will be unassigned.");
  return null;
}

// ─── Broker Partner: Build ticket description ────────────────────
function buildBrokerTicketDescription(formData) {
  const lines = [
    "━━━ Contact Information ━━━",
    `Name: ${formData.firstName} ${formData.lastName}`,
    `Email: ${formData.email || (formData.invalidEmail ? `${formData.invalidEmail} (rejected as invalid — verify with lead)` : "—")}`,
    `Phone: ${formData.phone}`,
    `Company: ${formData.company}`,
    "",
    "━━━ Broker Details ━━━",
  ];

  if (formData.states) lines.push(`States: ${formData.states}`);
  if (formData.monthlyVolume) lines.push(`Monthly Volume: ${formData.monthlyVolume}`);
  if (formData.loanProducts) lines.push(`Loan Products of Interest: ${formData.loanProducts}`);

  if (formData.notes) {
    lines.push("");
    lines.push("━━━ Notes ━━━");
    lines.push(formData.notes);
  }

  lines.push("");
  lines.push(`Submitted: ${new Date().toLocaleString("en-US", { timeZone: "America/New_York" })}`);
  lines.push(`Source: ${formData.pageUrl || "ledgertc.com/broker-partner"}`);

  return lines.join("\n");
}

// ─── Broker Partner: Build plain-text broker block for `message` contact property ──
// Renders inside the #inbounds Slack post via the workflow template's conditional
// `Message: {message}` line, so all broker details are visible in Slack at a glance.
function buildBrokerMessageProperty(formData) {
  const parts = [];
  if (formData.states) parts.push(`States: ${formData.states}`);
  if (formData.monthlyVolume) parts.push(`Monthly Volume: ${formData.monthlyVolume}`);
  if (formData.loanProducts) parts.push(`Loan Products: ${formData.loanProducts}`);
  if (formData.notes) {
    if (parts.length) parts.push("");
    parts.push(formData.notes);
  }
  return parts.join("\n");
}

async function patchBrokerMessageOnContact(contactId, formData) {
  if (!contactId) return;
  const message = buildBrokerMessageProperty(formData);
  if (!message) return;
  const result = await hubspot("PATCH", `/crm/v3/objects/contacts/${contactId}`, {
    properties: { message },
  });
  if (result.error) {
    console.error(`Failed to patch broker message on contact ${contactId}:`, result.data);
  } else {
    console.log(`Patched broker message onto contact ${contactId}`);
  }
}

// ─── Broker Partner: Build contact-note body (HTML for Activities tab) ──
function buildBrokerNoteBody(formData) {
  const lines = [];
  lines.push(`<b>Broker Partner Form Submission</b>`);
  lines.push("");
  lines.push("<u>Broker Details</u>");
  if (formData.states) lines.push(`• States: ${formData.states}`);
  if (formData.monthlyVolume) lines.push(`• Monthly Volume: ${formData.monthlyVolume}`);
  if (formData.loanProducts) lines.push(`• Loan Products of Interest: ${formData.loanProducts}`);
  if (formData.notes) {
    lines.push("");
    lines.push("<u>Notes</u>");
    lines.push(formData.notes.replace(/\n/g, "<br>"));
  }
  lines.push("");
  lines.push(`<i>Source: ${formData.pageUrl || "ledgertc.com/broker-partners"}</i>`);
  return lines.join("<br>");
}

// ─── Broker Partner: Create contact-timeline note ─────────────────
async function createBrokerContactNote(contactId, formData) {
  if (!contactId) return;
  const body = buildBrokerNoteBody(formData);
  const note = await hubspot("POST", "/crm/v3/objects/notes", {
    properties: {
      hs_note_body: body,
      hs_timestamp: Date.now(),
    },
    associations: [
      {
        to: { id: contactId },
        types: [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 202 }],
      },
    ],
  });
  if (note.error) {
    console.error(`Broker note creation failed for contact ${contactId}:`, note.data);
    return;
  }
  console.log(`Broker note logged to contact ${contactId} (note ${note.id})`);
}

// ─── Broker Partner: Create ticket ───────────────────────────────
async function createBrokerTicket(formData, ownerId, contactId, companyId) {
  const description = buildBrokerTicketDescription(formData);
  const ticketName = `Broker Inquiry - ${formData.firstName} ${formData.lastName} (${formData.company})`;
  const priority = formData.monthlyVolume === "10+ loans/month" ? "HIGH" : "MEDIUM";

  const properties = {
    subject: ticketName,
    content: description,
    hs_pipeline: process.env.HUBSPOT_PIPELINE_ID,
    hs_pipeline_stage: "1",
    hs_ticket_priority: priority,
    hs_ticket_category: "Broker_Inquiry",
  };

  if (ownerId) {
    properties.hubspot_owner_id = ownerId;
  }

  const ticket = await hubspot("POST", "/crm/v3/objects/tickets", { properties });

  if (ticket.error) {
    throw new Error(`Failed to create broker ticket: ${JSON.stringify(ticket.data)}`);
  }

  console.log(`Broker ticket created: ${ticket.id} (priority: ${priority})`);

  // Associate ticket to contact
  if (contactId) {
    await hubspot(
      "PUT",
      `/crm/v4/objects/tickets/${ticket.id}/associations/contacts/${contactId}`,
      [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 16 }]
    );
    console.log(`Broker ticket ${ticket.id} associated to contact ${contactId}`);
  }

  // Associate ticket to company
  if (companyId) {
    await hubspot(
      "PUT",
      `/crm/v4/objects/tickets/${ticket.id}/associations/companies/${companyId}`,
      [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 26 }]
    );
    console.log(`Broker ticket ${ticket.id} associated to company ${companyId}`);
  }

  return ticket;
}

// ─── Broker Partner: Send notification email ─────────────────────
async function sendBrokerNotificationEmail(formData, ticketId) {
  const subject = `New Broker Inquiry - ${formData.firstName} ${formData.lastName} | ${formData.company}`;
  const body = [
    `A new broker partner inquiry has been submitted.`,
    "",
    `Ticket ID: ${ticketId}`,
    "",
    "━━━ Contact Information ━━━",
    `Name: ${formData.firstName} ${formData.lastName}`,
    `Email: ${formData.email}`,
    `Phone: ${formData.phone}`,
    `Company: ${formData.company}`,
    "",
    "━━━ Broker Details ━━━",
    `States: ${formData.states || "—"}`,
    `Monthly Volume: ${formData.monthlyVolume || "—"}`,
    `Loan Products of Interest: ${formData.loanProducts || "—"}`,
    `Notes: ${formData.notes || "—"}`,
  ].join("\n");

  try {
    await fetch("https://api.mailchannels.net/tx/v1/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        personalizations: [{ to: [{ email: "Russell@ledgertc.com" }] }],
        from: { email: "noreply@ledgertc.com", name: "Ledger TC Website" },
        subject,
        content: [{ type: "text/plain", value: body }],
      }),
    });
    console.log("Broker notification email sent to Russell@ledgertc.com");
  } catch (err) {
    console.error("Failed to send broker notification email:", err);
  }
}

// ─── Step 5: Build ticket summary ────────────────────────────────
function buildTicketSummary(formData) {
  const lines = [
    "━━━ Contact Information ━━━",
    `Name: ${formData.firstName} ${formData.lastName}`,
    `Email: ${formData.email || (formData.invalidEmail ? `${formData.invalidEmail} (rejected as invalid — verify with lead)` : "—")}`,
    `Phone: ${formData.phone}`,
  ];

  if (formData.company) {
    lines.push(`Company: ${formData.company}`);
  }

  lines.push("");
  lines.push("━━━ Inquiry Details ━━━");

  if (formData.loanTypeInterest) lines.push(`Loan Type of Interest: ${formData.loanTypeInterest}`);
  if (formData.propertyTypeInterest) lines.push(`Property Type: ${formData.propertyTypeInterest}`);
  if (formData.propertyState) lines.push(`Subject Property State: ${formData.propertyState}`);
  if (formData.loanAmountRange) lines.push(`Loan Amount Range: ${formData.loanAmountRange}`);
  if (formData.dealTimeline) lines.push(`Deal Timeline: ${formData.dealTimeline}`);
  if (formData.loanType) lines.push(`Loan Type: ${formData.loanType}`);
  if (formData.loanAmount) lines.push(`Loan Amount: ${formData.loanAmount}`);
  if (formData.experience) lines.push(`Experience: ${formData.experience}`);
  if (formData.propertyAddress) lines.push(`Property Address: ${formData.propertyAddress}`);

  if (formData.projectOverview) {
    lines.push("");
    lines.push("━━━ Project Overview ━━━");
    lines.push(formData.projectOverview);
  }

  lines.push("");
  lines.push(`Submitted: ${new Date().toLocaleString("en-US", { timeZone: "America/New_York" })}`);
  lines.push(`Source: ${formData.pageUrl || "ledgertc.co/contact"}`);

  return lines.join("\n");
}

// ─── Step 6: Create Ticket ───────────────────────────────────────
async function createTicket(formData, ownerId, contactId, companyId, ticketCategory) {
  const summary = buildTicketSummary(formData);
  const ticketName = formData.company
    ? `Inbound Inquiry - ${formData.firstName} ${formData.lastName} (${formData.company})`
    : `Inbound Inquiry - ${formData.firstName} ${formData.lastName}`;

  const properties = {
    subject: ticketName,
    content: summary,
    hs_pipeline: process.env.HUBSPOT_PIPELINE_ID,
    hs_pipeline_stage: "1",  // "New" stage — update this if your stage ID differs
    hs_ticket_priority: "MEDIUM",
    hs_ticket_category: ticketCategory || "GENERAL_INQUIRY",
  };

  if (ownerId) {
    properties.hubspot_owner_id = ownerId;
  }

  const ticket = await hubspot("POST", "/crm/v3/objects/tickets", { properties });

  if (ticket.error) {
    throw new Error(`Failed to create ticket: ${JSON.stringify(ticket.data)}`);
  }

  console.log(`Ticket created: ${ticket.id}`);

  // Associate ticket to contact
  if (contactId) {
    await hubspot(
      "PUT",
      `/crm/v4/objects/tickets/${ticket.id}/associations/contacts/${contactId}`,
      [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 16 }]
    );
    console.log(`Ticket ${ticket.id} associated to contact ${contactId}`);
  }

  // Associate ticket to company
  if (companyId) {
    await hubspot(
      "PUT",
      `/crm/v4/objects/tickets/${ticket.id}/associations/companies/${companyId}`,
      [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 26 }]
    );
    console.log(`Ticket ${ticket.id} associated to company ${companyId}`);
  }

  return ticket;
}

// ─── Calc activity note helpers (for calc-first/form-after flow) ──
// When a visitor runs the calculator BEFORE submitting a form, the hutk
// has no contact attached yet so log-calc.js can't create the Note.
// In that case the calc payload is stashed in sessionStorage and posted
// up alongside the form submit as `pending_calc_log`. We retroactively
// create the Note here, after the contact has been created/found.

function calcFmtMoney(n) {
  const v = Number(n);
  if (!isFinite(v)) return String(n || "");
  return "$" + Math.round(v).toLocaleString();
}
function calcFmtPct(n, digits) {
  const v = Number(n);
  if (!isFinite(v)) return "";
  return v.toFixed(digits == null ? 1 : digits) + "%";
}

function buildDscrCalcNote(p) {
  const lines = [];
  const rate = p.rate != null ? Number(p.rate).toFixed(3) + "%" : "—";
  const amt = p.loan_amount != null ? calcFmtMoney(p.loan_amount) : "—";
  lines.push(`<b>DSCR Calculator — ${rate} on ${amt} loan</b>`);
  lines.push("");
  lines.push("<u>Inputs</u>");
  if (p.property_type) lines.push(`• Property: ${p.property_type}${p.state ? " (" + p.state + ")" : ""}`);
  if (p.loan_purpose) lines.push(`• Purpose: ${p.loan_purpose}`);
  if (p.arv != null) lines.push(`• Property Value: ${calcFmtMoney(p.arv)}`);
  if (p.monthly_rent != null) lines.push(`• Rent: ${calcFmtMoney(p.monthly_rent)}/mo`);
  const tii = [];
  if (p.annual_taxes != null) tii.push(`Taxes ${calcFmtMoney(p.annual_taxes)}/yr`);
  if (p.annual_insurance != null) tii.push(`Insurance ${calcFmtMoney(p.annual_insurance)}/yr`);
  if (p.annual_hoa) tii.push(`HOA ${calcFmtMoney(p.annual_hoa)}/yr`);
  if (tii.length) lines.push(`• ${tii.join(" | ")}`);
  if (p.fico != null) lines.push(`• FICO: ${p.fico}`);
  if (p.ltv != null) lines.push(`• LTV: ${calcFmtPct(Number(p.ltv) * 100, 0)}`);
  if (p.foreign_national) lines.push(`• Foreign National: ${p.foreign_national}`);
  if (p.interest_only) lines.push(`• Interest-Only: ${p.interest_only}`);
  if (p.prepay_months) lines.push(`• Prepay: ${p.prepay_months}-mo`);
  lines.push("");
  lines.push("<u>Result</u>");
  lines.push(`• Rate: ${rate}`);
  lines.push(`• Loan Amount: ${amt}`);
  if (p.monthly_pitia != null) lines.push(`• Monthly PITIA: ${calcFmtMoney(p.monthly_pitia)}`);
  if (p.dscr_ratio != null) lines.push(`• DSCR: ${Number(p.dscr_ratio).toFixed(2)}`);
  if (p.program_tier) lines.push(`• Program: ${p.program_tier}`);
  return lines.join("<br>");
}

function buildRtlCalcNote(p) {
  const lines = [];
  const rate = p.rate != null ? Number(p.rate).toFixed(3) + "%" : "—";
  const tla = p.tla != null ? calcFmtMoney(p.tla) : "—";
  lines.push(`<b>Construction Calculator — ${rate} on ${tla} TLA</b>`);
  lines.push("");
  lines.push("<u>Inputs</u>");
  if (p.program) lines.push(`• Program: ${p.program}`);
  if (p.asset_type) lines.push(`• Asset: ${p.asset_type}`);
  if (p.deal_type) lines.push(`• Deal: ${p.deal_type}`);
  if (p.project_type) lines.push(`• Project: ${p.project_type}`);
  if (p.experience) lines.push(`• Experience: ${p.experience}${p.experience_count != null ? " (" + p.experience_count + " projects)" : ""}`);
  if (p.fico != null) lines.push(`• FICO: ${p.fico}`);
  if (p.foreign_national) lines.push(`• Foreign National: ${p.foreign_national}`);
  if (p.purchase_price != null) lines.push(`• Purchase Price: ${calcFmtMoney(p.purchase_price)}`);
  if (p.as_is_value != null) lines.push(`• As-Is Value: ${calcFmtMoney(p.as_is_value)}`);
  if (p.rehab_budget != null && Number(p.rehab_budget) > 0) lines.push(`• Rehab Budget: ${calcFmtMoney(p.rehab_budget)}`);
  if (p.arv != null) lines.push(`• ARV: ${calcFmtMoney(p.arv)}`);
  if (p.loan_term_months) lines.push(`• Term: ${p.loan_term_months}mo`);
  lines.push("");
  lines.push("<u>Result</u>");
  lines.push(`• Rate: ${rate} (Tier ${p.tier != null ? p.tier : "—"})`);
  lines.push(`• Total Loan Amount: ${tla}`);
  if (p.day1 != null) lines.push(`• Day 1 Funding: ${calcFmtMoney(p.day1)}`);
  if (p.holdback != null && Number(p.holdback) > 0) lines.push(`• Construction Holdback: ${calcFmtMoney(p.holdback)}`);
  if (p.monthly_interest != null) lines.push(`• Monthly Interest: ${calcFmtMoney(p.monthly_interest)}`);
  const lev = [];
  if (p.ltaiv != null) lev.push(`LTAIV ${p.ltaiv}%`);
  if (p.ltc != null) lev.push(`LTC ${p.ltc}%`);
  if (p.ltv != null) lev.push(`LTARV ${p.ltv}%`);
  if (lev.length) lines.push(`• Leverage: ${lev.join(" · ")}`);
  return lines.join("<br>");
}

async function maybeLogPendingCalcNote(contactId, pendingCalcLogStr) {
  if (!pendingCalcLogStr || !contactId) return;
  let payload;
  try {
    payload = JSON.parse(pendingCalcLogStr);
  } catch (e) {
    console.warn("pending_calc_log: invalid JSON, skipping");
    return;
  }
  if (!payload || (payload.calculator !== "dscr" && payload.calculator !== "rtl")) {
    console.warn("pending_calc_log: unrecognized calculator, skipping");
    return;
  }
  const body = payload.calculator === "dscr"
    ? buildDscrCalcNote(payload)
    : buildRtlCalcNote(payload);

  const note = await hubspot("POST", "/crm/v3/objects/notes", {
    properties: {
      hs_note_body: body,
      hs_timestamp: Date.now(),
    },
    associations: [
      {
        to: { id: contactId },
        types: [{ associationCategory: "HUBSPOT_DEFINED", associationTypeId: 202 }],
      },
    ],
  });
  if (note.error) {
    console.error(`pending_calc_log: note creation failed for contact ${contactId}:`, note.data);
    return;
  }
  console.log(`pending_calc_log: logged ${payload.calculator} calc activity to contact ${contactId} (note ${note.id})`);
}

// ─── Meta Conversions API: server-side Lead event ────────────────
// Highest-match-quality path because we have unhashed email/phone here.
// Fire-and-forget pattern: errors logged but never break form submission.
// Skipped if FB_CAPI_TOKEN is not set (graceful no-op).
function sha256Lower(s) {
  return crypto.createHash("sha256").update(String(s).trim().toLowerCase()).digest("hex");
}
function normalizePhone(p) {
  const digits = String(p || "").replace(/\D/g, "");
  return digits ? sha256Lower(digits) : null;
}
async function sendMetaCapiLead(formData, eventReq) {
  const token = process.env.FB_CAPI_TOKEN;
  if (!token) {
    console.log("Meta CAPI: FB_CAPI_TOKEN not set, skipping");
    return;
  }
  try {
    const user_data = {};
    if (formData.email) user_data.em = [sha256Lower(formData.email)];
    if (formData.phone) {
      const ph = normalizePhone(formData.phone);
      if (ph) user_data.ph = [ph];
    }
    if (formData.firstName) user_data.fn = [sha256Lower(formData.firstName)];
    if (formData.lastName) user_data.ln = [sha256Lower(formData.lastName)];
    const clientIp = (eventReq.headers["x-forwarded-for"] || "").split(",")[0].trim();
    if (clientIp) user_data.client_ip_address = clientIp;
    const ua = eventReq.headers["user-agent"] || eventReq.headers["User-Agent"];
    if (ua) user_data.client_user_agent = ua;
    // fbp/fbc not collected in v1 — visitors aren't Meta-sourced yet (campaigns
    // still drafts). Add when draft campaigns publish: read _fbp cookie + build
    // fbc from fbclid query param, post both as hidden form fields.

    const body = {
      data: [{
        event_name: "Lead",
        event_time: Math.floor(Date.now() / 1000),
        action_source: "website",
        event_source_url: formData.pageUrl || "https://ledgertc.com/",
        user_data,
        custom_data: {
          currency: "USD",
          value: 0,
          lead_event_source: formData.formSource || "unknown",
        },
      }],
      access_token: token,
    };
    if (process.env.FB_CAPI_TEST_CODE) {
      body.test_event_code = process.env.FB_CAPI_TEST_CODE;
    }
    const res = await fetch(FB_CAPI_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const result = await res.json();
    if (result.error) {
      console.error("Meta CAPI error:", JSON.stringify(result.error));
    } else {
      console.log(`Meta CAPI Lead fired (events_received=${result.events_received})`);
    }
  } catch (err) {
    console.error("Meta CAPI exception:", err);
  }
}

// ─── Main handler ────────────────────────────────────────────────
exports.handler = async function (event) {
  // CORS headers
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  // Handle preflight
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: "" };
  }

  // Only accept POST
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: "Method not allowed" }),
    };
  }

  // ── Referer check (block if present and doesn't match) ────────
  const referer = event.headers["referer"] || event.headers["Referer"] || "";
  if (referer && !referer.includes("ledgertc.com") && !referer.includes("ledgertc.co")) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: "Forbidden" }) };
  }

  // Context captured for the catch-block Slack alert AND for the per-attempt
  // logger (errorCtx is hoisted because formData is const-scoped to try).
  const errorCtx = { email: "?", formSource: "?" };

  // Wraps every return inside the try block so that the outcome is logged to
  // #nervo_raw_subs before the response goes back to the caller. Adds ~50-100ms
  // per request; that's fine compared to the HubSpot round-trip already happening.
  const finishWith = async (outcome, response) => {
    await logRawSubmission(outcome, errorCtx);
    return response;
  };

  try {
    // Parse URL-encoded form data
    const params = new URLSearchParams(event.body);
    const raw = Object.fromEntries(params.entries());

    // Map snake_case form field names to camelCase
    const formData = {
      firstName: raw.first_name || raw.firstname || raw['first-name'] || "",
      lastName: raw.last_name || raw.lastname || raw['last-name'] || "",
      email: raw.email || "",
      phone: raw.phone || "",
      company: raw.company || "",
      loanType: raw.loan_type || "",
      loanAmount: raw.loan_amount || "",
      experience: raw.experience || "",
      propertyAddress: raw.property_address || "",
      projectOverview: raw.project_overview || raw.details || "",
      loanTypeInterest: raw.loan_type_interest || "",
      propertyTypeInterest: raw.property_type_interest || "",
      propertyState: raw.property_state || "",
      loanAmountRange: raw.loan_amount_range || "",
      dealTimeline: raw.deal_timeline || "",
      constructionType: raw.construction_type || "",
      website: raw.website || "",
      pageUrl: raw.page_url || "",
      gclid: raw.gclid || "",
      gbraid: raw.gbraid || "",
      wbraid: raw.wbraid || "",
      gadSource: raw.gad_source || "",
      gadCampaignId: raw.gad_campaignid || "",
      utmSource: raw.utm_source || "",
      utmMedium: raw.utm_medium || "",
      utmCampaign: raw.utm_campaign || "",
      utmTerm: raw.utm_term || "",
      utmContent: raw.utm_content || "",
      hutk: raw.hutk || "",
      pageName: raw.page_name || "",
      pendingCalcLog: raw.pending_calc_log || "",
    };

    // Resolve Google click ID: gclid > gbraid > wbraid
    formData.googleClickId = formData.gclid || formData.gbraid || formData.wbraid || "";
    const formSource = raw.form_source || "";
    formData.adsLp = raw.ads_lp || "";
    formData.isGoogleAds = !!(formData.googleClickId || formData.gadSource || formData.gadCampaignId || formSource.includes("google-ads") || formData.adsLp);

    // Campaign attribution from form_source and/or UTMs
    formData.formSource = raw.form_source || "";
    formData.adCampaign = deriveAdCampaign(formData.formSource, formData.utmCampaign, formData.adsLp, formData.isGoogleAds);

    // Capture context for the catch-block Slack alert
    errorCtx.email = formData.email || "?";
    errorCtx.formSource = formData.formSource || "?";
    errorCtx.name = `${formData.firstName} ${formData.lastName}`.trim();
    errorCtx.phone = formData.phone;
    errorCtx.projectOverview = formData.projectOverview;

    // Calculator results and project details -> project_details contact property
    const calcSummary = formatCalculatorResults(raw["calculator-results"] || "");
    const projectText = formData.projectOverview || "";
    formData.projectDetails = [calcSummary, projectText].filter(Boolean).join(" | ") || "";

    // Calculator submissions: backfill the 5 structured fields from the
    // calc JSON so calc leads populate the same HubSpot props as form leads.
    // HTML form selections take precedence; only fill blanks.
    const calcExtracted = extractCalculatorFields(formData.formSource, raw["calculator-results"] || "");
    for (const [k, v] of Object.entries(calcExtracted)) {
      if (!formData[k] && v) formData[k] = v;
    }

    // ── Honeypot check ──────────────────────────────────────────
    if (formData.website) {
      console.log("Honeypot triggered — rejecting silently");
      return finishWith("honeypot_blocked", {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, message: "Thank you for your submission." }),
      });
    }

    // ── Cloudflare Turnstile verification ───────────────────────
    const turnstileToken = raw["cf-turnstile-response"] || "";
    if (!turnstileToken) {
      console.log("Missing Turnstile token — rejecting");
      return finishWith("turnstile_missing", {
        statusCode: 403,
        headers,
        body: JSON.stringify({ error: "Security verification failed. Please refresh the page and try again." }),
      });
    }

    const turnstileRes = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        secret: process.env.TURNSTILE_SECRET_KEY,
        response: turnstileToken,
        remoteip: (event.headers["x-forwarded-for"] || "").split(",")[0].trim(),
      }),
    });
    const turnstileData = await turnstileRes.json();

    if (!turnstileData.success) {
      console.log("Turnstile verification failed:", turnstileData);
      return finishWith("turnstile_failed", {
        statusCode: 403,
        headers,
        body: JSON.stringify({ error: "Security verification failed. Please refresh the page and try again." }),
      });
    }

    // ── Timestamp check (reject if present and under 3 seconds) ─
    const formLoadedAt = raw.form_loaded_at;
    if (!formLoadedAt || isNaN(Number(formLoadedAt)) || (Date.now() - Number(formLoadedAt)) < 3000) {
      console.log("Timestamp check failed — submission too fast");
      return finishWith("timestamp_too_fast", {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: "Submission too fast" }),
      });
    }

    // ── IP rate limiting (5 per hour) ───────────────────────────
    const clientIp = (event.headers["x-forwarded-for"] || event.headers["client-ip"] || "unknown")
      .split(",")[0].trim();

    if (clientIp !== "unknown") {
      const recentCount = pruneAndCount(clientIp);
      if (recentCount >= IP_MAX) {
        console.warn(`Rate limit exceeded for IP ${clientIp} (${recentCount} in last hour)`);
        return finishWith("rate_limited", {
          statusCode: 429,
          headers,
          body: JSON.stringify({ error: "Too many submissions. Please try again later." }),
        });
      }
      recordIp(clientIp);
    }

    // ── Daily submission cap (100/day) ──────────────────────────
    const todayTotal = checkAndIncrementDaily();
    if (todayTotal > DAILY_CAP) {
      if (todayTotal === DAILY_CAP + 1) {
        await sendDailyCapAlert();
      }
      console.warn(`Daily cap exceeded: ${todayTotal} submissions today`);
      return finishWith("daily_cap_exceeded", {
        statusCode: 503,
        headers,
        body: JSON.stringify({ error: "We've received a high volume of inquiries today. Please try again tomorrow or email us directly at Info@ledgertc.com." }),
      });
    }

    // Calculator forms (rtl-calculator, dscr-calculator) accept email OR phone,
    // matching the UI promise. All other forms require both.
    const isCalcForm = formData.formSource === "rtl-calculator" || formData.formSource === "dscr-calculator";

    // Validate required fields
    const required = isCalcForm
      ? ["firstName", "lastName"]
      : ["firstName", "lastName", "email", "phone"];
    const missing = required.filter((f) => !formData[f] || formData[f].trim() === "");
    if (missing.length > 0) {
      return finishWith("validation_missing_fields", {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: `Missing required fields: ${missing.join(", ")}` }),
      });
    }
    if (isCalcForm && !formData.email && !formData.phone) {
      return finishWith("validation_no_email_or_phone", {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: "Please provide either an email address or phone number." }),
      });
    }

    // Basic email validation (only when email is provided)
    if (formData.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email)) {
      return finishWith("validation_invalid_email", {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: "Invalid email address" }),
      });
    }

    // ── Disposable / spam email domain blocking ───────────────
    if (formData.email) {
      const emailDomain = formData.email.split("@")[1].toLowerCase();
      if (DISPOSABLE_EMAIL_DOMAINS.has(emailDomain)) {
        console.log(`Disposable email domain blocked: ${emailDomain} (${formData.email})`);
        return finishWith("disposable_email_blocked", {
          statusCode: 200,
          headers,
          body: JSON.stringify({ success: true, message: "Thank you for your submission." }),
        });
      }
    }

    console.log(`Processing submission from ${formData.email}`);

    // ── Broker Partner flow ─────────────────────────────────────
    if (raw.form_source === "broker-partner") {
      // Map broker-specific fields
      // states is now a multi-select (checkboxes). Object.fromEntries() only
      // keeps the last one, so pull from params directly.
      formData.states = params.getAll("states").filter(Boolean).join(", ");
      formData.monthlyVolume = raw.monthly_volume || "";
      // loan_products is a multi-select (checkboxes). Same pattern.
      formData.loanProducts = params.getAll("loan_products").filter(Boolean).join(", ");
      formData.notes = raw.notes || "";
      // Pre-compute the `message` contact property so it's set at contact-create
      // time (the #inbounds Slack workflow fires on Contact Created and reads
      // `enrolled_object.message`).
      formData.message = buildBrokerMessageProperty(formData);

      // Step 1: Contact
      const contactResult = await findOrCreateContact(formData);
      const contactId = contactResult.contact.id;

      // Associate hutk for HubSpot visitor tracking
      await associateHutkWithContact(formData.email, formData.hutk, formData.pageUrl, formData.pageName);

      // Register form submission via Forms API (creates "Submitted form: X" timeline event)
      await registerFormSubmission(formData);

      // Re-assert tracking properties after hutk association (hutk can overwrite hs_analytics_source)
      await ensureTrackingAfterHutk(contactId, formData);

      // Step 2: Company — check existing association first, then find/create
      let companyResult = null;
      if (!contactResult.isNew) {
        companyResult = await getContactCompany(contactId, formData);
      }
      if (!companyResult) {
        const companyName = formData.company || `${formData.firstName} ${formData.lastName} LLC`;
        companyResult = await findOrCreateCompany(companyName, formData);
      }
      const companyId = companyResult.company ? companyResult.company.id : null;

      // Step 3: Associate Contact <-> Company (only if company is NEW)
      if (companyId && companyResult.isNew && contactId) {
        await associateContactToCompany(contactId, companyId);
      }

      // Step 4: Determine ticket owner
      const ownerId = await determineTicketOwner(contactResult, companyResult);

      // Step 5: Create broker ticket
      const ticket = await createBrokerTicket(formData, ownerId, contactId, companyId);

      // Step 5b: Mirror broker details onto contact's Activities tab as a Note
      await createBrokerContactNote(contactId, formData);

      // Step 5c: Write broker block to `message` property so the HubSpot
      // workflow that posts #inbounds picks it up via its `Message:` line.
      await patchBrokerMessageOnContact(contactId, formData);

      // Step 6: Send notification email
      await sendBrokerNotificationEmail(formData, ticket.id);

      // Step 7: Fire Meta CAPI Lead event (highest match quality from server)
      await sendMetaCapiLead(formData, event);

      return finishWith("success_broker", {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: "Your broker partner inquiry has been submitted. Our team will follow up within one business day.",
          ticketId: ticket.id,
        }),
      });

    // ── Standard contact form flow ──────────────────────────────
    } else {
      // Step 1: Contact
      const contactResult = await findOrCreateContact(formData);
      const contactId = contactResult.contact.id;

      // Associate hutk for HubSpot visitor tracking
      await associateHutkWithContact(formData.email, formData.hutk, formData.pageUrl, formData.pageName);

      // Register form submission via Forms API (creates "Submitted form: X" timeline event)
      await registerFormSubmission(formData);

      // Re-assert tracking properties after hutk association (hutk can overwrite hs_analytics_source)
      await ensureTrackingAfterHutk(contactId, formData);

      // Write new structured form fields (degrades gracefully if properties don't yet exist)
      await patchContactWithFormFields(contactId, formData);

      // Calc-first/form-after: if visitor ran the calculator before submitting
      // this form, log-calc.js couldn't attach a Note (no contact existed for
      // that hutk yet). The browser stashed the calc payload and posted it up
      // here as `pending_calc_log` — create the Note now.
      if (formData.pendingCalcLog) {
        try {
          await maybeLogPendingCalcNote(contactId, formData.pendingCalcLog);
        } catch (err) {
          console.error("pending_calc_log handling failed:", err);
        }
      }

      // Step 2: Company — check existing association first, then find/create
      let companyResult = null;
      if (!contactResult.isNew) {
        companyResult = await getContactCompany(contactId, formData);
      }
      if (!companyResult) {
        const companyName = formData.company || `${formData.firstName} ${formData.lastName} LLC`;
        companyResult = await findOrCreateCompany(companyName, formData);
      }
      const companyId = companyResult.company ? companyResult.company.id : null;

      // Step 3: Associate Contact <-> Company (only if company is NEW)
      if (companyId && companyResult.isNew && contactId) {
        await associateContactToCompany(contactId, companyId);
      }

      // Step 4: Determine ticket owner
      const ownerId = await determineTicketOwner(contactResult, companyResult);

      // Step 5 & 6: Create ticket with summary and associations
      const ticketCategoryMap = {
        "construction-landing-page-google-ads": "Campaign",
        "first-time-builder-landing-page-google-ads": "First Time Campaign",
        "first-time-builder-landing-page": "First Time Campaign",
        "dscr-landing-page-google-ads": "DSCR Campaign",
        "fix-and-flip-landing-page-google-ads": "Fix and Flip Campaign",
        "ledger-vs-kiavi-lp": "Conquest Campaign",
        "ledger-vs-civic-financial-services-lp": "Conquest Campaign",
        "ledger-vs-lima-one-capital-lp": "Conquest Campaign",
        "ledger-vs-renovo-financial-lp": "Conquest Campaign",
        "ledger-vs-conventus-lp": "Conquest Campaign",
        "ledger-vs-cofi-lending-lp": "Conquest Campaign",
        "ledger-vs-housemax-funding-lp": "Conquest Campaign",
        "ledger-vs-groundfloor-lp": "Conquest Campaign",
        "ledger-vs-easy-street-capital-lp": "Conquest Campaign",
        "ledger-vs-temple-view-capital-lp": "Conquest Campaign",
        "compare-lenders-landing-page": "Conquest Campaign",
      };
      const ticketCategory = ticketCategoryMap[raw.form_source] || "GENERAL_INQUIRY";
      const ticket = await createTicket(formData, ownerId, contactId, companyId, ticketCategory);

      // Step 7: Fire Meta CAPI Lead event (highest match quality from server)
      await sendMetaCapiLead(formData, event);

      return finishWith("success", {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: "Your application has been submitted. Our team will follow up within one business day.",
          ticketId: ticket.id,
        }),
      });
    }

  } catch (err) {
    console.error("Error processing form submission:", err);

    // Real-time error alert to #nervo_ops (high-visibility, for on-call).
    // The per-attempt logger below also captures this in #nervo_raw_subs
    // for the weekly audit count.
    if (process.env.SLACK_WEBHOOK_URL) {
      fetch(process.env.SLACK_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          text: [
            `:rotating_light: *submit-form ERROR* — \`${errorCtx.formSource}\` — ${errorCtx.email} — ${err && err.message ? err.message : String(err)}`,
            `Name: ${errorCtx.name || "?"} — Phone: ${errorCtx.phone || "?"}`,
            ...(errorCtx.projectOverview ? [`> ${String(errorCtx.projectOverview).slice(0, 500)}`] : []),
          ].join("\n"),
        }),
      }).catch(slackErr => console.error("Slack error alert failed:", slackErr));
    }

    // Per-attempt log (same channel as every other outcome)
    errorCtx.err = err && err.message ? err.message : String(err);
    await logRawSubmission("error", errorCtx);

    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: "Something went wrong. Please try again or contact us directly.",
      }),
    };
  }
};
