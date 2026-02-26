# CLAUDE.md — KonvoAI Customer Success

## What this project is
Python scripts + n8n automation workflows that power the KonvoAI CS team's operations:
reporting dashboards, churn tracking, issue management, and customer health monitoring.

## Credentials
All API keys live in `Credentials.md` (gitignored — never commit it).
Reference pattern: `open('Credentials.md').read()` and parse the relevant section.

---

## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Automation | n8n Cloud (`konvoai.app.n8n.cloud`) | Workflows triggered by webhooks / cron |
| CRM | Notion | Master Customer Table (MCT) + Issues Table |
| Billing | Stripe | Subscription status, MRR, churn events |
| Support | Intercom | Customer conversations, reply-time KPIs |
| Product issues | Linear | Bug/feature tracking, synced to Notion |
| Marketing CRM | HubSpot | Lead/contact enrichment |
| Analytics | ClickHouse → BigQuery | Weekly customer KPI snapshots |
| Alerts | Slack | `#customer-success-core` (incoming webhook, no bot needed) |
| Language | Python 3 | All scripts; JS only inside n8n Code nodes |

---

## Key Notion IDs

```
Master Customer Table (MCT) DB:  84feda19cfaf4c6e9500bf21d2aaafef
MCT Data Source ID:              3ceb1ad0-91f1-40db-945a-c51c58035898
Issues Table DB:                 bd1ed48de20e426f8bebeb8e700d19d8
Scorecard DB:                    311e418f-d8c4-810e-8b11-cdc50357e709
```

## Key n8n workflow IDs

```
Stripe Churn: Update Billing Status   8cLtcqxjD8DC59JG  (webhook)
Stripe → Notion Sync (Daily)          Ai9Y3FWjqMtEhr57  (cron 09:30 CET)
Linear → Notion Issue Sync            xdVkUh6YCtcuW8QM  (webhook)
Intercom Catch-up Poller              J1l8oI22H26f9iM5  (cron every 30min)
Weekly Scorecard Builder              eUwMYFeglyv9bHxn  (cron Mon 06:00 CET)
Customers Contacted Tracker           iDA5BBJxsp0cmv2M  (cron daily)
Daily Last Contact Date Sync          veEIgePuCQ0z9jYr  (cron 23:30 CET)
GCal Onboarding → Notion To-Dos       hRo3wsttFHUdU3jo  (cron daily 07:30 CET)
```

---

## Notion API critical rules

- **MCT uses a multi-source database** — always query via `POST /data_sources/{DS_ID}/query`
  with `Notion-Version: 2025-09-03`. The standard `databases/{id}/query` fails.
- **MCT page PATCH** also requires `Notion-Version: 2025-09-03`.
- **Issues Table** works normally with `databases/{id}/query` + `2022-06-28`.
- **Property type changes on MCT** — use `PATCH /data_sources/{DS_ID}` (not `/databases/{id}`).
  Never set a property to `null` in a data_sources PATCH — it can silently corrupt other properties.

## MCT key columns (Master Customer Table)

```
🏢 Company Name       title
💰 Billing Status     select: Active | Churning | Canceled
💰 MRR                number
🔁 Churn Reason       select: Missing features | AI Behavior | Platform & UI |
                               Integration | Competitor | Unknown
😢 Churn Date         date  — when subscription actually ended
📅 Cancel Date        date  — when subscription will end (Stripe cancel_at)
📅 Churning Since     date  — when customer clicked cancel (Stripe canceled_at)
🏢 Domain             rich_text
🔗 Stripe Customer ID rich_text
📞 Last Contact Date 🔒  date  — system-managed, do not edit manually
⭐ CS Owner           select: Alex | Aya
```

## Issues Table key columns

```
Issue Title    title
Category       select → 5 buckets: Feature request | AI Behavior | Integration |
                                    Platform & UI | Billing & Account
Issue Type     select — used for in/out-of-scope filtering
Status         select: Open | In Progress | Resolved
Created At     date
Resolved At    date
Assigned To    rollup of CS Owner from Customer relation
```

---

## n8n development rules

- **HTTP Request nodes only for API calls** — never `this.helpers.httpRequest()` in Code nodes
- **Code nodes = data processing only**; use `runOnceForAllItems` + `.all()` for cross-item work
- **Branch scoping**: `$('NodeName').all()` on a branch only sees items on that branch path.
  Fix: stamp required fields into each item BEFORE any IF node.
- **`}}` inside n8n template strings** (`={{ "key": "{{ expr }}" }}`) causes early close.
  Fix: use pure expression mode (`={{ JSON.stringify({...}) }}`) or split as `'}' + '}'`.
- **Webhook nodes**: use `typeVersion: 2` + include `webhookId: "<uuid>"`.
  Must flip the toggle in the n8n UI after API deployment to register the webhook.
- **Workflow activation**: use `POST /activate` — PATCH with `{"active":true}` returns 405.
- **PUT /workflows/{id}**: only `name`, `nodes`, `connections`, `settings` accepted.

## Stripe patterns

- Churning = `cancel_at_period_end: true` → fires `customer.subscription.updated`
- `canceled_at` = when customer clicked cancel (decision date) ← use for period tracking
- `cancel_at` = when subscription will end (billing end date)
- Canceled = `status: "canceled"` → fires `customer.subscription.updated`

---

## Meeting report pipeline

```
fetch_report_data.py   →  report_data.json  →  generate_meeting_report.py  →  meeting_report.html
```

- Reporting periods: P1 Feb 16–Mar 1 / P2 Mar 2–15 / P3 Mar 16–29 (hardcoded in `fetch_report_data.py`)
- In-scope issues: assigned to Alex or Aya, not type `No Issue` or `Config Issue`
- Resolution snapshots frozen at P_start + 7d/14d/28d, stored in `resolution_snapshots.json`
- Churn tracking: uses `📅 Churning Since` (decision date) for period bucketing, not `📅 Cancel Date`
- To update for a new cycle: change `PERIOD_RANGES` and `CURRENT_PERIOD` in `fetch_report_data.py`

## Output conventions

- Dashboards: single-file HTML with Chart.js + chartjs-plugin-datalabels (CDN), dark-on-light,
  fullscreen slides navigated by arrow keys / dot nav
- Scripts: standalone Python 3, parse credentials from `Credentials.md`, print progress to stdout
- No ORM, no dependencies beyond `requests` + stdlib unless strictly necessary
- Archive one-time/fix scripts to `archive/` once deployed
