/* form-validation.js — Ledger Trade & Capital
 *
 * Shared, light-touch client-side input guardrails for the public lead forms.
 * Loaded with `defer` on each form page. It does TWO things and nothing else:
 *
 *   1. Email — structural format check: exactly one "@" with text on both sides,
 *      a real ".tld" (>=2 letters), no spaces, no leading/trailing/double dots.
 *      Blocks submit with an inline error when malformed.
 *
 *   2. Phone — formats "(XXX) XXX-XXXX" as you type and requires a complete
 *      10-digit (or leading-1 + 10) number on submit. This is ONLY wired up on
 *      forms that don't already have their own inline phone validation. Pages
 *      whose template already validates phone expose `window._ledgerValidation`
 *      or `window._brokerPhoneValidation`; we detect that and leave phone alone,
 *      so the ~15 forms that already do this are never double-bound.
 *
 * What it deliberately does NOT do:
 *   - It never makes a blank field required. A field is validated only when it
 *     has a value, so every form keeps exactly whatever its HTML marks required.
 *   - No deliverability / disposable-domain / API checks. Structure only.
 *   - No server-side behavior changes.
 *
 * The submit gate is registered at the document level in the CAPTURE phase, so
 * it runs before each page's own submit handler and can block an invalid
 * submission (preventDefault + stopImmediatePropagation) without the page's
 * fetch ever firing.
 */
(function () {
  if (window.__ledgerFormValidationLoaded) return;
  window.__ledgerFormValidationLoaded = true;

  // A page already handles phone if its inline template set either global.
  var hasInlinePhone = !!(window._ledgerValidation || window._brokerPhoneValidation);

  var ERR_STYLE = 'color:#dc2626; font-size:0.8rem; margin-top:0.25rem;';

  // ── Email rule ──────────────────────────────────────────────────────────
  function isValidEmail(raw) {
    var v = String(raw == null ? '' : raw).trim();
    if (!v) return true;                              // empty → not our concern
    if (/\s/.test(v)) return false;                   // no spaces
    var parts = v.split('@');
    if (parts.length !== 2) return false;             // exactly one @
    var local = parts[0], domain = parts[1];
    if (!local || !domain) return false;              // text on both sides
    if (/^\.|\.$|\.\./.test(local)) return false;     // no edge/double dots
    if (/^\.|\.$|\.\./.test(domain)) return false;
    return /^[^.]+(\.[^.]+)*\.[A-Za-z]{2,}$/.test(domain); // real .tld
  }

  // ── Phone rule (identical to the existing per-page template) ────────────
  function isValidPhone(raw) {
    var digits = String(raw == null ? '' : raw).replace(/[^\d]/g, '');
    return /^1?\d{10}$/.test(digits);
  }
  function formatPhoneRealtime(e) {
    var input = e.target;
    var d = input.value.replace(/\D/g, '').substring(0, 10);
    var out = '';
    if (d.length > 6) out = '(' + d.substring(0, 3) + ') ' + d.substring(3, 6) + '-' + d.substring(6);
    else if (d.length > 3) out = '(' + d.substring(0, 3) + ') ' + d.substring(3);
    else if (d.length > 0) out = '(' + d;
    input.value = out;
  }

  // ── Inline error element (one per input, tracked on the input itself) ───
  function ensureErr(input, msg) {
    if (input._lvErr) return input._lvErr;
    var el = document.createElement('div');
    el.style.cssText = ERR_STYLE;
    el.textContent = msg;
    if (input.parentNode) input.parentNode.insertBefore(el, input.nextSibling);
    input._lvErr = el;
    return el;
  }
  function showErr(input) { if (input._lvErr) input._lvErr.style.display = 'block'; }
  function hideErr(input) { if (input._lvErr) input._lvErr.style.display = 'none'; }

  // ── Field wiring ────────────────────────────────────────────────────────
  function each(list, fn) { Array.prototype.forEach.call(list, fn); }

  function init() {
    each(document.querySelectorAll('form input[type="email"]'), function (inp) {
      if (inp.dataset.lvEmail) return;
      inp.dataset.lvEmail = '1';
      var err = ensureErr(inp, 'Please enter a valid email address');
      err.style.display = 'none';
      inp.addEventListener('blur', function () { isValidEmail(inp.value) ? hideErr(inp) : showErr(inp); });
      inp.addEventListener('input', function () { if (isValidEmail(inp.value)) hideErr(inp); });
    });

    if (hasInlinePhone) return; // phone already owned by the page's own script

    each(document.querySelectorAll('form input[type="tel"]'), function (inp) {
      if (inp.dataset.lvPhone) return;
      inp.dataset.lvPhone = '1';
      var err = ensureErr(inp, 'Please enter a valid phone number');
      err.style.display = 'none';
      inp.addEventListener('input', formatPhoneRealtime);
      // only validate when the field has a value — never makes phone required
      inp.addEventListener('blur', function () { (!inp.value.trim() || isValidPhone(inp.value)) ? hideErr(inp) : showErr(inp); });
    });
  }

  // ── Submit gate (capture phase, before the page's own handlers) ─────────
  function gate(e) {
    var form = e.target;
    if (!form || form.nodeName !== 'FORM') return;
    var firstBad = null;

    each(form.querySelectorAll('input[type="email"][data-lv-email]'), function (inp) {
      if (!isValidEmail(inp.value)) { showErr(inp); firstBad = firstBad || inp; }
    });
    if (!hasInlinePhone) {
      each(form.querySelectorAll('input[type="tel"][data-lv-phone]'), function (inp) {
        if (inp.value.trim() && !isValidPhone(inp.value)) { showErr(inp); firstBad = firstBad || inp; }
      });
    }

    if (firstBad) {
      e.preventDefault();
      e.stopImmediatePropagation();
      try { firstBad.focus(); } catch (_) {}
    }
  }

  document.addEventListener('submit', gate, true);

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
