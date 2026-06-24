// Ledger Trade & Capital - HubSpot Cookie Keeper (ITP mitigation)
// Netlify Function: /hsk  (rewritten from /.netlify/functions/hsk via netlify.toml)
//
// PROBLEM
//   Safari ITP caps JavaScript-set first-party cookies (the way HubSpot's
//   tracking script writes `hubspotutk` via document.cookie) to 7 days. A
//   builder who researches Ledger, gets ITP-wiped, and submits weeks later
//   arrives with a fresh/absent hutk on a referrer-less session, so HubSpot
//   mints a NEW contact stamped DIRECT_TRAFFIC instead of recognizing the
//   returning visitor and preserving their real first-touch source.
//
// FIX (mirrors the GA4/AW first-party sGTM cookie persistence on tag.ledgertc.com)
//   Keep a durable, server-set, HttpOnly companion cookie `__hs_utk_keep` that
//   Safari cannot truncate the way it truncates JS-set cookies. On every visit:
//     - if a live hubspotutk is present  -> copy/refresh it into the keeper
//     - if hubspotutk is gone but keeper survives (the ITP-wiped return visit)
//       -> restore hubspotutk from the keeper BEFORE HubSpot's script reads it
//   HubSpot then resolves the returning visitor to the same anonymous identity,
//   and submit-form.js's existing hutk association re-attributes the contact to
//   its true original source instead of falling back to DIRECT_TRAFFIC.
//
// This is purely additive. If /hsk errors or is slow, the page snippet loads
// HubSpot exactly as before (behavior is unchanged for new visitors).

const KEEPER = "__hs_utk_keep"; // durable, HttpOnly, server-set companion
const UTK = "hubspotutk"; // HubSpot's own cookie (must stay JS-readable)
const MAX_AGE = 60 * 60 * 24 * 180; // 180 days, rolling

// hubspotutk is a 32-char lowercase hex string. Keep validation strict enough
// to forbid header injection (no ; , whitespace, CR/LF) but tolerant of any
// reasonable future format HubSpot might use.
const UTK_RE = /^[A-Za-z0-9._-]{8,128}$/;

function parseCookies(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(";")) {
    const i = part.indexOf("=");
    if (i < 0) continue;
    const k = part.slice(0, i).trim();
    const v = part.slice(i + 1).trim();
    if (k) out[k] = decodeURIComponent(v);
  }
  return out;
}

// Build a Set-Cookie header value. Host-only (no Domain) to match HubSpot's
// default apex cookie and avoid creating a duplicate hubspotutk on a different
// scope. HttpOnly only for the keeper (HubSpot's JS must read hubspotutk).
function setCookie(name, value, { httpOnly }) {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    "Path=/",
    `Max-Age=${MAX_AGE}`,
    "Secure",
    "SameSite=Lax",
  ];
  if (httpOnly) parts.push("HttpOnly");
  return parts.join("; ");
}

exports.handler = async function (event) {
  const baseHeaders = {
    "Cache-Control": "no-store, private",
    Vary: "Cookie",
  };

  // Only GET/POST do work; never error out on anything else (additive, safe).
  const method = event.httpMethod || "GET";
  if (method === "OPTIONS") {
    return { statusCode: 204, headers: baseHeaders, body: "" };
  }

  const cookies = parseCookies(event.headers.cookie || event.headers.Cookie || "");
  const utk = cookies[UTK];
  const keep = cookies[KEEPER];

  const setCookies = [];
  let action = "noop"; // noop | capture | restore

  if (utk && UTK_RE.test(utk)) {
    // Live hutk present: persist/refresh it into the durable keeper. Do NOT
    // re-set hubspotutk itself — HubSpot owns that cookie while it's alive.
    if (keep !== utk) action = "capture";
    setCookies.push(setCookie(KEEPER, utk, { httpOnly: true }));
  } else if (keep && UTK_RE.test(keep)) {
    // ITP wiped the 7-day hubspotutk but the durable keeper survived.
    // Restore hubspotutk from it (JS-readable) and roll the keeper window.
    setCookies.push(setCookie(UTK, keep, { httpOnly: false }));
    setCookies.push(setCookie(KEEPER, keep, { httpOnly: true }));
    action = "restore";
  }
  // else: brand-new visitor with no hutk yet — HubSpot's script will mint one,
  // and the next pageview's `capture` branch will persist it.

  const headers = { ...baseHeaders, "X-HSK-Action": action };
  const response = { statusCode: 200, headers, body: JSON.stringify({ ok: true, action }) };
  if (setCookies.length) {
    response.multiValueHeaders = { "Set-Cookie": setCookies };
  }
  return response;
};
