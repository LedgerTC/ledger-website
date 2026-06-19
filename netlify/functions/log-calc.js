// Ledger Trade & Capital - Calculator Activity Logger
// Netlify Function: /.netlify/functions/log-calc
//
// Receives a calculator-rate-calculated event from the website calculator pages
// and, IF the visitor's hubspotutk cookie resolves to an existing HubSpot
// contact, creates a Note engagement on that contact's timeline summarizing
// the inputs and computed result. Anonymous visitors (no hutk match) are
// silently skipped — GA4 still has the event-level history.
//
// Environment variables required (set in Netlify dashboard):
//   HUBSPOT_TOKEN  - Private app token (same one submit-form.js uses)

const HUBSPOT_API = "https://api.hubapi.com";

const ipHits = new Map();
const IP_MAX = 30;
const IP_WINDOW_MS = 60 * 60 * 1000;

function pruneAndCount(ip) {
  const now = Date.now();
  const cutoff = now - IP_WINDOW_MS;
  const ts = (ipHits.get(ip) || []).filter((t) => t > cutoff);
  ipHits.set(ip, ts);
  return ts.length;
}
function recordIp(ip) {
  const ts = ipHits.get(ip) || [];
  ts.push(Date.now());
  ipHits.set(ip, ts);
}

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
    return { error: true, status: res.status, data };
  }
  return data;
}

async function getContactIdByHutk(hutk) {
  if (!hutk) return null;
  const profile = await hubspot("GET", `/contacts/v1/contact/utk/${encodeURIComponent(hutk)}/profile`);
  if (profile && !profile.error && profile.vid) return String(profile.vid);
  return null;
}

function fmtMoney(n) {
  const v = Number(n);
  if (!isFinite(v)) return String(n || "");
  return "$" + Math.round(v).toLocaleString();
}
function fmtPct(n, digits) {
  const v = Number(n);
  if (!isFinite(v)) return "";
  return v.toFixed(digits == null ? 1 : digits) + "%";
}

function buildDscrNoteBody(p) {
  const lines = [];
  const rate = p.rate != null ? Number(p.rate).toFixed(3) + "%" : "—";
  const amt = p.loan_amount != null ? fmtMoney(p.loan_amount) : "—";
  lines.push(`<b>DSCR Calculator — ${rate} on ${amt} loan</b>`);
  lines.push("");
  lines.push("<u>Inputs</u>");
  if (p.property_address) lines.push(`• Address: ${p.property_address}`);
  if (p.property_type) lines.push(`• Property: ${p.property_type}${p.state ? " (" + p.state + ")" : ""}`);
  if (p.loan_purpose) lines.push(`• Purpose: ${p.loan_purpose}`);
  if (p.arv != null) lines.push(`• Property Value: ${fmtMoney(p.arv)}`);
  if (p.monthly_rent != null) lines.push(`• Rent: ${fmtMoney(p.monthly_rent)}/mo`);
  const tii = [];
  if (p.annual_taxes != null) tii.push(`Taxes ${fmtMoney(p.annual_taxes)}/yr`);
  if (p.annual_insurance != null) tii.push(`Insurance ${fmtMoney(p.annual_insurance)}/yr`);
  if (p.annual_hoa) tii.push(`HOA ${fmtMoney(p.annual_hoa)}/yr`);
  if (tii.length) lines.push(`• ${tii.join(" | ")}`);
  if (p.fico != null) lines.push(`• FICO: ${p.fico}`);
  if (p.ltv != null) lines.push(`• LTV: ${fmtPct(Number(p.ltv) * 100, 0)}`);
  if (p.foreign_national) lines.push(`• Foreign National: ${p.foreign_national}`);
  if (p.interest_only) lines.push(`• Interest-Only: ${p.interest_only}`);
  if (p.prepay_months) lines.push(`• Prepay: ${p.prepay_months}-mo`);
  lines.push("");
  lines.push("<u>Result</u>");
  lines.push(`• Rate: ${rate}`);
  lines.push(`• Loan Amount: ${amt}`);
  if (p.monthly_pitia != null) lines.push(`• Monthly PITIA: ${fmtMoney(p.monthly_pitia)}`);
  if (p.dscr_ratio != null) lines.push(`• DSCR: ${Number(p.dscr_ratio).toFixed(2)}`);
  if (p.program_tier) lines.push(`• Program: ${p.program_tier}`);
  return lines.join("<br>");
}

function buildRtlNoteBody(p) {
  const lines = [];
  const rate = p.rate != null ? Number(p.rate).toFixed(3) + "%" : "—";
  const tla = p.tla != null ? fmtMoney(p.tla) : "—";
  lines.push(`<b>Construction Calculator — ${rate} on ${tla} TLA</b>`);
  lines.push("");
  lines.push("<u>Inputs</u>");
  if (p.property_address) lines.push(`• Address: ${p.property_address}`);
  if (p.program) lines.push(`• Program: ${p.program}`);
  if (p.asset_type) lines.push(`• Asset: ${p.asset_type}`);
  if (p.deal_type) lines.push(`• Deal: ${p.deal_type}`);
  if (p.project_type) lines.push(`• Project: ${p.project_type}`);
  if (p.experience) lines.push(`• Experience: ${p.experience}${p.experience_count != null ? " (" + p.experience_count + " projects)" : ""}`);
  if (p.fico != null) lines.push(`• FICO: ${p.fico}`);
  if (p.foreign_national) lines.push(`• Foreign National: ${p.foreign_national}`);
  if (p.purchase_price != null) lines.push(`• Purchase Price: ${fmtMoney(p.purchase_price)}`);
  if (p.as_is_value != null) lines.push(`• As-Is Value: ${fmtMoney(p.as_is_value)}`);
  if (p.rehab_budget != null && Number(p.rehab_budget) > 0) lines.push(`• Rehab Budget: ${fmtMoney(p.rehab_budget)}`);
  if (p.arv != null) lines.push(`• ARV: ${fmtMoney(p.arv)}`);
  if (p.loan_term_months) lines.push(`• Term: ${p.loan_term_months}mo`);
  lines.push("");
  lines.push("<u>Result</u>");
  lines.push(`• Rate: ${rate} (Tier ${p.tier != null ? p.tier : "—"})`);
  lines.push(`• Total Loan Amount: ${tla}`);
  if (p.day1 != null) lines.push(`• Day 1 Funding: ${fmtMoney(p.day1)}`);
  if (p.holdback != null && Number(p.holdback) > 0) lines.push(`• Construction Holdback: ${fmtMoney(p.holdback)}`);
  if (p.monthly_interest != null) lines.push(`• Monthly Interest: ${fmtMoney(p.monthly_interest)}`);
  const lev = [];
  if (p.ltaiv != null) lev.push(`LTAIV ${p.ltaiv}%`);
  if (p.ltc != null) lev.push(`LTC ${p.ltc}%`);
  if (p.ltv != null) lev.push(`LTARV ${p.ltv}%`);
  if (lev.length) lines.push(`• Leverage: ${lev.join(" · ")}`);
  return lines.join("<br>");
}

async function createNoteOnContact(contactId, body) {
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
  return note;
}

exports.handler = async function (event) {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers, body: "" };
  }
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  }

  const referer = event.headers["referer"] || event.headers["Referer"] || "";
  if (referer && !referer.includes("ledgertc.com") && !referer.includes("ledgertc.co")) {
    return { statusCode: 403, headers, body: JSON.stringify({ error: "Forbidden" }) };
  }

  const ip = (event.headers["x-forwarded-for"] || event.headers["client-ip"] || "unknown")
    .split(",")[0].trim();
  if (ip !== "unknown") {
    if (pruneAndCount(ip) >= IP_MAX) {
      return { statusCode: 200, headers, body: JSON.stringify({ success: true, skipped: "rate_limited" }) };
    }
    recordIp(ip);
  }

  let req;
  try {
    req = JSON.parse(event.body || "{}");
  } catch {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) };
  }

  const hutk = req.hutk || "";
  const payload = req.payload || {};

  if (!hutk) {
    return { statusCode: 200, headers, body: JSON.stringify({ success: true, skipped: "no_hutk" }) };
  }

  const calculator = payload.calculator;
  if (calculator !== "dscr" && calculator !== "rtl") {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid calculator" }) };
  }

  try {
    const contactId = await getContactIdByHutk(hutk);
    if (!contactId) {
      return { statusCode: 200, headers, body: JSON.stringify({ success: true, skipped: "no_contact_for_hutk" }) };
    }

    const noteBody = calculator === "dscr"
      ? buildDscrNoteBody(payload)
      : buildRtlNoteBody(payload);

    const note = await createNoteOnContact(contactId, noteBody);
    if (note.error) {
      console.error(`Note creation failed for contact ${contactId}:`, note.data);
      return { statusCode: 200, headers, body: JSON.stringify({ success: false, error: "note_creation_failed" }) };
    }

    console.log(`Logged ${calculator} calc activity to contact ${contactId} (note ${note.id})`);
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ success: true, contactId, noteId: note.id }),
    };
  } catch (err) {
    console.error("log-calc error:", err);
    return { statusCode: 200, headers, body: JSON.stringify({ success: false, error: "internal_error" }) };
  }
};
