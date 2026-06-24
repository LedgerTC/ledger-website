// Unit tests for the HubSpot cookie-keeper (netlify/functions/hsk.js).
// Run: node tests/test_hsk.js
const { handler } = require("../netlify/functions/hsk.js");

let pass = 0, fail = 0;
function ok(cond, msg) {
  if (cond) { pass++; console.log(`  ✓ ${msg}`); }
  else { fail++; console.log(`  ✗ ${msg}`); }
}
async function call(cookie, method) {
  return handler({ httpMethod: method || "GET", headers: cookie ? { cookie } : {} });
}
function getSetCookies(res) {
  return (res.multiValueHeaders && res.multiValueHeaders["Set-Cookie"]) || [];
}
function findCookie(setCookies, name) {
  return setCookies.find((c) => c.startsWith(name + "="));
}
const VALID = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"; // 32-char hex like a real hutk

(async () => {
  console.log("CASE 1: live hubspotutk present, no keeper yet -> capture into keeper");
  let res = await call(`hubspotutk=${VALID}`);
  let sc = getSetCookies(res);
  ok(JSON.parse(res.body).action === "capture", "action = capture");
  ok(!!findCookie(sc, "__hs_utk_keep"), "sets __hs_utk_keep");
  ok(findCookie(sc, "__hs_utk_keep").includes(VALID), "keeper holds the hutk value");
  ok(findCookie(sc, "__hs_utk_keep").includes("HttpOnly"), "keeper is HttpOnly");
  ok(findCookie(sc, "__hs_utk_keep").includes("Max-Age=15552000"), "keeper 180d");
  ok(!findCookie(sc, "hubspotutk"), "does NOT re-set hubspotutk while it is alive");

  console.log("\nCASE 2: keeper already matches live hutk -> refresh only (noop action)");
  res = await call(`hubspotutk=${VALID}; __hs_utk_keep=${VALID}`);
  sc = getSetCookies(res);
  ok(JSON.parse(res.body).action === "noop", "action = noop (already captured)");
  ok(!!findCookie(sc, "__hs_utk_keep"), "still rolls keeper Max-Age");
  ok(!findCookie(sc, "hubspotutk"), "leaves hubspotutk untouched");

  console.log("\nCASE 3: ITP wiped hubspotutk, keeper survives -> restore");
  res = await call(`__hs_utk_keep=${VALID}`);
  sc = getSetCookies(res);
  ok(JSON.parse(res.body).action === "restore", "action = restore");
  ok(!!findCookie(sc, "hubspotutk"), "re-issues hubspotutk");
  ok(findCookie(sc, "hubspotutk").includes(VALID), "restored hutk = original value");
  ok(!findCookie(sc, "hubspotutk").includes("HttpOnly"), "restored hubspotutk is JS-readable (not HttpOnly)");
  ok(!!findCookie(sc, "__hs_utk_keep"), "rolls keeper window too");

  console.log("\nCASE 4: brand-new visitor, no cookies -> noop, no Set-Cookie");
  res = await call("");
  ok(JSON.parse(res.body).action === "noop", "action = noop");
  ok(getSetCookies(res).length === 0, "no Set-Cookie headers");

  console.log("\nCASE 5: malformed/injection hutk -> ignored");
  res = await call(`hubspotutk=${encodeURIComponent("evil; Path=/; Domain=.evil.com")}`);
  ok(JSON.parse(res.body).action === "noop", "rejects non-conforming value");
  ok(getSetCookies(res).length === 0, "no cookie set for bad value");

  console.log("\nCASE 6: response is cache-safe");
  res = await call(`hubspotutk=${VALID}`);
  ok((res.headers["Cache-Control"] || "").includes("no-store"), "Cache-Control no-store");
  ok((res.headers["Vary"] || "").includes("Cookie"), "Vary: Cookie");

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
