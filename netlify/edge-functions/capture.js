// Ledger Trade & Capital — First-party attribution capture (ITP mitigation)
// Netlify Edge Function: runs at the edge, in the path of every page request.
// Stamps a durable, SERVER-SET, first-party cookie (`ltc_attr`) holding the ad
// click IDs / UTMs from the landing URL.
//
// WHY EDGE + SERVER-SET
//   Safari ITP truncates JavaScript-set cookies (and sessionStorage) to ~7 days,
//   so the old sessionStorage capture loses a returning visitor's gclid/fbclid.
//   A cookie set by the SERVER (this function's response header) is not subject
//   to that cap, so it survives ~90 days across tabs, restarts, and return
//   visits. Same intent as hsk.js (hutk) and the Google sGTM first-party cookie
//   on tag.ledgertc.com — this one is for our OWN click IDs.
//
//   submit-form.js reads `ltc_attr` server-side from the request cookie header
//   and uses it to backfill click IDs / UTMs the form POST didn't carry.
//
// Purely additive & fail-safe: any error just passes the page through unchanged.

const TRACKING_KEYS = [
  "gclid", "gbraid", "wbraid", "fbclid", "msclkid",
  "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
];
const COOKIE = "ltc_attr";
const MAX_AGE = 60 * 60 * 24 * 90; // 90 days ≈ ad conversion window

export default async (request, context) => {
  try {
    const url = new URL(request.url);

    // Collect tracking params present on THIS landing URL.
    const found = {};
    for (const k of TRACKING_KEYS) {
      const v = url.searchParams.get(k);
      if (v) found[k] = v.slice(0, 512); // bound size; guards against abuse
    }

    // No ad params here -> nothing to capture; leave the request untouched.
    // (This is the common case for organic pageviews and internal navigation.)
    if (Object.keys(found).length === 0) return;

    found.ts = Date.now(); // last-touch timestamp

    // Last-touch: a new tracked landing replaces the stored set (matches the
    // prior sessionStorage behavior). encodeURIComponent on the whole JSON
    // blob prevents any cookie-header injection from param values.
    const value = encodeURIComponent(JSON.stringify(found));

    const res = await context.next();
    // Host-only (no Domain) so it works on whatever host serves it (.com/.co),
    // matching hsk.js. HttpOnly: only submit-form.js reads it, server-side.
    res.headers.append(
      "Set-Cookie",
      `${COOKIE}=${value}; Path=/; Max-Age=${MAX_AGE}; Secure; SameSite=Lax; HttpOnly`,
    );
    return res;
  } catch (_e) {
    // Never let capture break a page load.
    return;
  }
};
