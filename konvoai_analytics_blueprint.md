# KonvoAI Customer Analytics Pipeline — Blueprint

## Context

KonvoAI is a B2B AI platform automating customer service for ecommerce companies (~200 customers). This project builds a weekly analytics pipeline that extracts customer KPI snapshots from ClickHouse Cloud, stores them in Google BigQuery, and visualizes them in Looker Studio dashboards.

---

## What We've Done So Far

### ClickHouse Setup
- **Instance**: ClickHouse Cloud, hosted on AWS eu-central-1
- **Host**: `ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443`
- **Source table**: `operator.public_workspace_report_snapshot`
- **Key columns**: `stripe_customer_id`, `ai_resolution_rate`, `ai_sessions_count`, `ai_sessions_resolved`, `active_skills_count`, `channels_with_active_skills`, `open_tickets`, `report_date`, `created_at`, and more
- **Authentication**: API key `n8n edo` (Read-only, stored in Bitwarden) successfully authenticates via Basic Auth in n8n
- **Working SQL query** (saved in ClickHouse console as "Customers KPIs - Daily"):
  ```sql
  SELECT *
  FROM public_workspace_report_snapshot
  WHERE toDate(created_at) = (
      SELECT max(toDate(created_at))
      FROM public_workspace_report_snapshot
  )
  ```
- **n8n node**: HTTP Request node "Query ClickHouse" — POST to ClickHouse HTTP API, returns JSON with `meta` + `data` arrays

### n8n Workflow
- **Trigger**: Daily 4AM trigger (already configured)
- **Node 1**: Query ClickHouse — working, returns ~200 rows with all customer KPIs
- **Node 2**: Fetch Notion Pages — currently failing ("Databases with multiple data sources not supported")
- Workflow lives at: `konvoai.app.n8n.cloud`

---

## Architecture Decision

**Chosen approach: Google BigQuery + Looker Studio**

Instead of storing data in Notion (poor analytics performance) or ClickHouse (requires Scott), we write weekly aggregated snapshots to BigQuery and build dashboards in Looker Studio.

### Why this works
- BigQuery free tier: 10GB storage + 1TB queries/month — covers KonvoAI for years
- Looker Studio: free, powerful, shareable dashboards, native BigQuery connector
- Fully independent of engineering (Edoardo controls it end to end)
- Future: can also sync Notion tables into BigQuery for cross-source analytics

---

## Target Data Model

### BigQuery table: `customer_kpis_weekly`

| Column | Type | Description |
|--------|------|-------------|
| `week_start` | DATE | Monday of the reporting week |
| `stripe_customer_id` | STRING | Customer identifier |
| `org_id` | STRING | Organization ID |
| `workspace_id` | STRING | Workspace ID |
| `avg_ai_resolution_rate` | FLOAT | Weekly avg AI resolution rate |
| `avg_ai_sessions_count` | FLOAT | Weekly avg total AI sessions |
| `avg_ai_sessions_resolved` | FLOAT | Weekly avg resolved sessions |
| `avg_open_tickets` | FLOAT | Weekly avg open tickets |
| `avg_active_skills` | FLOAT | Weekly avg active skills |
| `avg_channels_active` | FLOAT | Weekly avg active channels |
| `data_points` | INT | Number of daily snapshots averaged |
| `ingested_at` | TIMESTAMP | When this row was written |

---

## Roadmap

### Phase 1 — BigQuery Setup (1–2 hours)
- [ ] Create a Google Cloud project (or use existing)
- [ ] Enable BigQuery API
- [ ] Create dataset: `konvoai_analytics`
- [ ] Create table `customer_kpis_weekly` with schema above
- [ ] Create a Service Account with BigQuery Data Editor role
- [ ] Download Service Account JSON key
- [ ] Add BigQuery credential to n8n (Google Service Account)

### Phase 2 — n8n Workflow Update (1–2 hours)
- [ ] Change trigger from Daily 4AM to Weekly (Monday 4AM)
- [ ] Update SQL query to return full week of data and aggregate:
  ```sql
  SELECT
      stripe_customer_id,
      org_id,
      toStartOfWeek(toDate(created_at)) AS week_start,
      avg(ai_resolution_rate) AS avg_ai_resolution_rate,
      avg(ai_sessions_count) AS avg_ai_sessions_count,
      avg(ai_sessions_resolved) AS avg_ai_sessions_resolved,
      avg(open_tickets) AS avg_open_tickets,
      avg(active_skills_count) AS avg_active_skills,
      avg(channels_with_active_skills) AS avg_channels_active,
      count() AS data_points
  FROM public_workspace_report_snapshot
  WHERE toDate(created_at) >= toStartOfWeek(today() - 7)
    AND toDate(created_at) < toStartOfWeek(today())
  GROUP BY stripe_customer_id, org_id, week_start
  ```
- [ ] Add Code node to transform ClickHouse JSON response into BigQuery row format
- [ ] Replace Notion node with BigQuery node (insert rows)
- [ ] Test end to end with a manual run
- [ ] Enable weekly schedule

### Phase 3 — Looker Studio Dashboard (2–3 hours)
- [ ] Connect Looker Studio to BigQuery dataset
- [ ] Build core dashboard with:
  - AI Resolution Rate trend per customer (line chart, weekly)
  - AI Sessions volume heatmap (all customers x weeks)
  - Top/bottom 10 customers by resolution rate (bar chart)
  - Customer health overview table (latest week snapshot)
  - Filters: by customer, by date range, by org
- [ ] Share dashboard with Guillem and team

### Phase 4 — Enrich with Notion Data (optional, future)
- [ ] Identify which Notion tables have useful data (e.g. customer success notes, MRR, churn risk)
- [ ] Build n8n workflow to sync Notion → BigQuery weekly
- [ ] Join Notion data with ClickHouse KPIs in Looker Studio for unified customer view

---

## Open Items / Dependencies
- **Scott**: No longer needed for ClickHouse read access (API key working). May need to whitelist n8n IP if ClickHouse adds IP restrictions later.
- **ClickHouse full column list**: Confirm all column names in `public_workspace_report_snapshot` before finalizing BigQuery schema (some columns were truncated in the UI).
- **Notion connector**: When ready for Phase 4, evaluate between a third-party Looker Studio Notion connector vs. syncing Notion → BigQuery via n8n (recommended for reliability and join capability).

---

## Credentials Reference (all stored in Bitwarden)
- ClickHouse API Key: `n8n edo` — Key ID starts with `fnyjDV...`
- ClickHouse host: `ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443`
- n8n workspace: `konvoai.app.n8n.cloud`
- BigQuery: Service Account JSON (to be created in Phase 1)

