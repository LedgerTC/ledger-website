# Changelog

All notable changes to the Ledger Trade & Capital website (ledgertc.com) and its
Netlify functions are recorded here.

This site is a continuously-deployed static site (no versioned releases), so entries
are grouped by date rather than semver. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/). Categories: **Added**, **Changed**,
**Fixed**, **Removed**, **Ops/Infra**.

Most recent first.

## [Unreleased]

Working-tree changes not yet committed/deployed.

### Added
- `CLAUDE.md` — project guidance for Claude Code (stack, architecture, conventions).
- `docs/form-submission-guide.md` — form submission flow documentation.

### Changed
- `index.html` — in-progress edits.
- `.claude/commands/deploy-staging.md`, `.claude/commands/deploy-production.md`,
  `docs/deploy-workflow.md` — deploy workflow doc/command tweaks.

### Investigation / Open items (not code changes)
- **HubSpot "Unregistered Site Domain" spam flagging** — confirmed analytics-only,
  **no lead loss** (reconciled 186/186 website contacts → all have tickets).
  Required fix is UI-only: add `ledgertc.com`, `www.ledgertc.com`, `ledgertc.co` to
  Advanced Tracking site domains. Open: a snapshot+diff script (via
  `propertiesWithHistory`) to safely test whether restoring one spam submission
  clobbers API-set attribution (`hs_analytics_source`) before any bulk restore.
- **Loan-app pipeline gap** — `/loan-application` → `submit-loan-app.js` is decoupled
  from HubSpot/Slack and broken in prod (missing `DOCUSEAL_API_KEY` → silent 502s).
- **#inbounds notification gap** — Slack alert fires on HubSpot "Contact Created", so
  submissions from existing contacts create a ticket but no #inbounds ping.

## 2026-06-17

### Added
- `form-validation.js` — shared, light-touch client-side input guardrails wired into
  22 lead forms via one `<script src="/form-validation.js" defer>` line. Two structural
  rules: email (single `@`, text both sides, real `.tld` ≥2 letters, no spaces/edge/
  double dots) on every form; phone format-as-you-type + 10-digit check added only on
  forms that don't already validate phone (detects `window._ledgerValidation` /
  `window._brokerPhoneValidation`). Validates a field only when it has a value, so no
  form's required set changes. Submit gate runs at the document level in the capture
  phase, before each page's own handler. `loan-application.html` excluded (own flow).

## 2026-06-12

### Fixed
- `netlify/functions/submit-form.js` — leads with emails that pass our regex but
  fail HubSpot validation (e.g. `gmail.comg`, 6/10 incident) are no longer dropped:
  on `INVALID_EMAIL`, contact creation retries phone-only with the rejected email
  noted in `project_details` and on the ticket. Error alerts (#nervo_ops) and the
  per-attempt error log (#nervo_raw_subs) now include name, phone, and project
  overview so any future drop is reconstructable from Slack alone.

## 2026-05-26

### Added
- Submission audit: `scripts/audit-submissions.js` — 6-layer reconciliation report
  with lightweight + detailed XLSX output and multi-channel posting.
- `submit-form.js` now logs every submission attempt to `#nervo_raw_subs` to feed the
  audit funnel.

## 2026-05-24 — 2026-05-25

### Added
- FTB landing page: required **Loan Purpose** dropdown to filter out owner-occupied
  submissions.
- Submission audit groundwork + Slack error alerts.

### Changed
- DSCR pages: explicit long-term-rentals-only language.

## 2026-05-21

### Changed
- Loan app: repeatable guarantors, relaxed required fields, condensed final-page legal
  disclosure.

## 2026-05-11 — 2026-05-14

### Added
- `/build-to-rent-lp` paid landing page.
- Meta Pixel + Conversions API for ledgertc.com (plus privacy-policy disclosure for
  Meta app review).

### Changed
- Broker form: surface details in the `#inbounds` Slack post and mirror them to the
  contact Activities tab.
- Forms: preserve typed project overview across calculator resubmits.

## 2026-05-04 — 2026-05-06

### Changed
- Route `gtag.js` through `tag.ledgertc.com` server-side GTM (transport_url, Option B)
  + sGTM companion pixel for ITP-resistant remarketing.
- FTB LP: add Requirements section above the fold.
- SEO Tier 2: title/meta tweaks on near-page-1 pages.
- Mobile perf: reorder form below content, lazy-render Turnstile.

### Fixed
- Calculator: fix Day 1 sizing for seasoned refis.

## 2026-04-22 — 2026-04-28

### Added
- Footprint expansion: 40 → 42 states (added Idaho + Utah).
- Test harness for `submit-form.js` validation logic.
- Calculator activity logged to HubSpot contact timeline.

### Changed
- Calculator: rebuilt rate/fee structure to target 2% net revenue; 5% buffer,
  cap-binds trim holdback first.
- SEO Tier 1: title/meta rewrites on near-page-1 pages.
- Geo/product page accuracy sweep: bridge buildout, thin state refreshes, Census 2025
  preliminary permits + Vintage 2024 metro population.
- Privacy policy / TOS: SMS/text messaging disclosures.

### Fixed
- Calc lead form: fix silent 400s on email-or-phone submissions.
- Forms API: re-enable timeline events after disabling Collected Forms.

### Ops/Infra
- Docs: CallRail → HubSpot attribution spec for the n8n workflow extension.
