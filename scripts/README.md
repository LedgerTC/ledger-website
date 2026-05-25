# scripts/

Operational scripts for the Ledger TC marketing site. These are **not** deployed
to Netlify — they run locally (or in CI) for audits, backfills, and one-offs.

## audit-submissions.js

Reconciles three HubSpot data layers over a date range and posts a summary plus
mismatch list to the `#nervo_ops` Slack channel.

The three layers:

1. **Contacts** — created/updated with `form_source` set (every contact touched
   by `submit-form.js`).
2. **Tickets** — created by `submit-form.js` (one per successful submission).
3. **HubSpot form submissions** — captured independently by the HubSpot
   tracking script (Forms API). Discrepancies here reveal the dual-capture
   issue tracked in `project_hubspot_dual_submission`.

### Mismatches it surfaces

- **HS form submission with no matching ticket** (±60 min) — either the
  tracking script captured a submission the function never received, or the
  function failed before creating the ticket.
- **Contact has `form_source` but no ticket** — the function crashed between
  the contact PATCH and ticket POST.
- **Duplicate HS form submissions** (same email within 5 min) — the
  dual-capture bug firing.
- **Duplicate tickets** (same email within 5 min) — user resubmitted or a
  retry storm.
- **Tickets with no associated contact** — data integrity issue.

### Setup

1. Create a `.env` file in the repo root (already `.gitignore`d via the existing
   `.gitignore` — verify before committing if you're unsure):

   ```
   HUBSPOT_TOKEN=pat-na1-xxxxxxxx
   HUBSPOT_PORTAL_ID=12345678
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxxxx
   ```

   The HubSpot token needs scopes: `crm.objects.contacts.read`,
   `crm.objects.tickets.read`, `forms` (to list form submissions).

   Get the Slack webhook URL from
   `https://api.slack.com/apps` → your app → Incoming Webhooks → Add to
   `#nervo_ops`.

2. Requires Node 20.6+ (for the `--env-file` flag). Check with `node -v`.

### Run

```sh
# All-time audit (since 2024-01-01)
node --env-file=.env scripts/audit-submissions.js

# Specific window
node --env-file=.env scripts/audit-submissions.js --from 2026-05-17 --to 2026-05-24

# Last 7 days (use this for the weekly job)
node --env-file=.env scripts/audit-submissions.js --weekly

# Print Slack payload to stdout instead of posting — use for first runs
node --env-file=.env scripts/audit-submissions.js --weekly --dry-run
```

### Recommended first-time flow

```sh
# 1. Verify everything works without spamming Slack
node --env-file=.env scripts/audit-submissions.js --weekly --dry-run

# 2. Once happy, post the all-time baseline once
node --env-file=.env scripts/audit-submissions.js

# 3. From then on, run weekly (see scheduling options below)
node --env-file=.env scripts/audit-submissions.js --weekly
```

### Scheduling options (for when you're ready)

The script is plain Node — wrap it in whichever scheduler you prefer:

| Host | Pros | Cons |
|---|---|---|
| **GitHub Action** (`schedule: cron`) | Version-controlled, easy debugging, can commit reports back | Needs secrets configured in repo settings |
| **n8n** (already on `n8n.ledgertc.co`) | Same place as CallRail workflow, visual UI for Russell | Workflow lives outside this repo |
| **Netlify scheduled function** | Same env vars as `submit-form.js`, no new infra | 10s timeout limits batch size; needs the function wrapper |

No structural changes needed to the script for any of these — only where the
`HUBSPOT_TOKEN` and `SLACK_WEBHOOK_URL` env vars live.

### Companion: persistent submission log

`netlify/functions/submit-form.js` writes every form-submission attempt to
Netlify Blobs at `submission-log/YYYY-MM-DD/<uuid>.json`. Captures attempts that
never reach HubSpot (honeypot blocks, rate limits, validation fails, function
errors). The audit script does not yet read these — once ~1 week of data has
accumulated, extend the script to compare "website attempts" vs. "HubSpot
received" for a true end-to-end view.

To read the blobs locally for spot-checking:

```sh
netlify blobs:list submission-log
netlify blobs:get submission-log 2026-05-24/<uuid>.json
```

(Requires `netlify login` first.)
