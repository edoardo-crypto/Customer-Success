# Claude Code Prompt: Build KonvoAI n8n Workflows (ClickHouse → BigQuery)

## Objective

Build TWO n8n workflows on `konvoai.app.n8n.cloud` that move customer KPI data from ClickHouse Cloud to Google BigQuery:

1. **Backfill workflow** (one-off, manual trigger): pulls ALL historical data from ClickHouse, aggregates by customer and week, inserts into BigQuery.
2. **Weekly append workflow** (recurring, Monday 4AM CET): pulls only the previous week's data, aggregates, appends to the same BigQuery table.

Both workflows write to the same BigQuery table: `konvoai-n8n.konvoai_analytics.customer_kpis_weekly`.

---

## Credentials

All API credentials are already saved in the n8n credentials tab at `konvoai.app.n8n.cloud`. You will also find them in the `credentials.md` file in your working directory.

### ClickHouse Cloud
- **Host**: `ua2wi80os4.eu-central-1.aws.clickhouse.cloud`
- **Port**: `8443` (HTTPS)
- **Authentication**: Basic Auth (API key named `n8n edo`, read-only). The username is the Key ID, the password is the Key Secret. These are stored in n8n credentials and in Bitwarden.
- **Source table**: `operator.public_workspace_report_snapshot`
- **HTTP API endpoint**: `https://ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443/`
- **Method**: POST, with the SQL query as the request body
- **Required query parameter**: `default_format=JSONEachRow` (or append `FORMAT JSONEachRow` to the SQL query)

### Google BigQuery
- **Credential name in n8n**: `Google BigQuery Service Account`
- **Project ID**: `konvoai-n8n`
- **Dataset**: `konvoai_analytics`
- **Table**: `customer_kpis_weekly`
- **Service Account**: `n8n-bigquery@konvoai-n8n.iam.gserviceaccount.com` (BigQuery Data Editor role)

---

## BigQuery Target Table Schema

The table `konvoai-n8n.konvoai_analytics.customer_kpis_weekly` already exists with this schema:

| Column | Type |
|--------|------|
| stripe_customer_id | STRING |
| org_id | STRING |
| workspace_id | STRING |
| week_start | DATE |
| avg_ai_resolution_rate | FLOAT64 |
| avg_ai_sessions_total | FLOAT64 |
| avg_ai_sessions_count | FLOAT64 |
| avg_ai_sessions_resolved | FLOAT64 |
| avg_ai_sessions_unresolved | FLOAT64 |
| avg_active_skills_count | FLOAT64 |
| avg_active_processes_count | FLOAT64 |
| avg_custom_replies_count | FLOAT64 |
| avg_channels_connected_count | FLOAT64 |
| avg_channels_with_ai_count | FLOAT64 |
| avg_test_scenarios_count | FLOAT64 |
| avg_open_tickets_count | FLOAT64 |
| avg_messages_sent24h | FLOAT64 |
| avg_messages_received24h | FLOAT64 |
| data_points | INT64 |
| ingested_at | TIMESTAMP |

---

## Workflow 1: Backfill (One-Off)

### Workflow name: `Backfill - ClickHouse to BigQuery`

### Node 1: Manual Trigger
- Type: Manual Trigger
- No configuration needed, just a button to click and run

### Node 2: Query ClickHouse
- Type: HTTP Request
- Method: POST
- URL: `https://ua2wi80os4.eu-central-1.aws.clickhouse.cloud:8443/`
- Authentication: Use the existing ClickHouse Basic Auth credential (named `n8n edo` or similar in credentials tab)
- Content-Type: `text/plain`
- Body (raw, single line, no line breaks):

```
SELECT stripe_customer_id, org_id, workspace_id, toMonday(toDate(created_at)) AS week_start, avg(ai_resolution_rate) AS avg_ai_resolution_rate, avg(ai_sessions_total) AS avg_ai_sessions_total, avg(ai_sessions_count) AS avg_ai_sessions_count, avg(ai_sessions_resolved) AS avg_ai_sessions_resolved, avg(ai_sessions_unresolved) AS avg_ai_sessions_unresolved, avg(active_skills_count) AS avg_active_skills_count, avg(active_processes_count) AS avg_active_processes_count, avg(custom_replies_count) AS avg_custom_replies_count, avg(channels_connected_count) AS avg_channels_connected_count, avg(channels_with_ai_count) AS avg_channels_with_ai_count, avg(test_scenarios_count) AS avg_test_scenarios_count, avg(open_tickets_count) AS avg_open_tickets_count, avg(messages_sent24h) AS avg_messages_sent24h, avg(messages_received24h) AS avg_messages_received24h, count() AS data_points FROM operator.public_workspace_report_snapshot GROUP BY stripe_customer_id, org_id, workspace_id, week_start FORMAT JSONEachRow
```

- CRITICAL: The response format must be `JSONEachRow` (not `JSON`), so n8n receives each row as a separate item. If using `FORMAT JSON`, n8n will see only 1 item containing a nested `data` array, which breaks downstream processing.
- Expected output: ~200 customers x ~52 weeks = ~10,000+ individual JSON items

### Node 3: Add Timestamp (Code Node)
- Type: Code (JavaScript)
- Mode: Run Once for Each Item (or "Run Once for All Items" depending on n8n version)
- Code:

```javascript
for (const item of $input.all()) {
  item.json.ingested_at = new Date().toISOString();
}
return $input.all();
```

- This adds the `ingested_at` timestamp to every row before inserting into BigQuery.

### Node 4: Insert to BigQuery
- Type: Google BigQuery
- Credential: `Google BigQuery Service Account`
- Operation: **Insert** (NOT "Execute Query")
- Project: `konvoai-n8n` (select by ID)
- Dataset: `konvoai_analytics`
- Table: `customer_kpis_weekly`
- Column mapping: Map ALL incoming fields from the Code node output to the corresponding BigQuery columns. The field names from ClickHouse match the BigQuery column names exactly, so this should be a direct 1:1 mapping.
- Important: If n8n's BigQuery node has a batch size limit, set it to the maximum allowed (e.g., 500 or 1000 rows per batch). The backfill may insert 10,000+ rows.

### Node connections:
Manual Trigger → Query ClickHouse → Add Timestamp → Insert to BigQuery

---

## Workflow 2: Weekly Append (Recurring)

### Workflow name: `Weekly - ClickHouse to BigQuery`

### Node 1: Schedule Trigger
- Type: Schedule Trigger
- Frequency: Weekly
- Day: Monday
- Time: 04:00 (4 AM)
- Timezone: Europe/Berlin (CET)

### Node 2: Query ClickHouse
- Identical to the backfill workflow EXCEPT the SQL query includes a date filter for only the previous week:

```
SELECT stripe_customer_id, org_id, workspace_id, toMonday(toDate(created_at)) AS week_start, avg(ai_resolution_rate) AS avg_ai_resolution_rate, avg(ai_sessions_total) AS avg_ai_sessions_total, avg(ai_sessions_count) AS avg_ai_sessions_count, avg(ai_sessions_resolved) AS avg_ai_sessions_resolved, avg(ai_sessions_unresolved) AS avg_ai_sessions_unresolved, avg(active_skills_count) AS avg_active_skills_count, avg(active_processes_count) AS avg_active_processes_count, avg(custom_replies_count) AS avg_custom_replies_count, avg(channels_connected_count) AS avg_channels_connected_count, avg(channels_with_ai_count) AS avg_channels_with_ai_count, avg(test_scenarios_count) AS avg_test_scenarios_count, avg(open_tickets_count) AS avg_open_tickets_count, avg(messages_sent24h) AS avg_messages_sent24h, avg(messages_received24h) AS avg_messages_received24h, count() AS data_points FROM operator.public_workspace_report_snapshot WHERE toDate(created_at) >= toMonday(today()) - 7 AND toDate(created_at) < toMonday(today()) GROUP BY stripe_customer_id, org_id, workspace_id, week_start FORMAT JSONEachRow
```

- This returns only data from the previous Monday-to-Sunday window.
- Expected output: ~200 rows (one per customer for that week)

### Node 3: Add Timestamp (Code Node)
- Identical to backfill workflow:

```javascript
for (const item of $input.all()) {
  item.json.ingested_at = new Date().toISOString();
}
return $input.all();
```

### Node 4: Insert to BigQuery
- Identical to backfill workflow. Same project, dataset, table, column mapping.

### Node connections:
Schedule Trigger → Query ClickHouse → Add Timestamp → Insert to BigQuery

---

## Implementation Approach

### Option A: n8n REST API (preferred if API key is available)
Use the n8n REST API to create both workflows programmatically:
- Endpoint: `https://konvoai.app.n8n.cloud/api/v1/workflows`
- Method: POST
- Auth: n8n API key (check credentials.md or the n8n settings)
- Body: Full workflow JSON definition

### Option B: Generate importable JSON files
If no API key is available, generate two JSON files that can be imported into n8n:
1. `backfill_workflow.json`
2. `weekly_workflow.json`

The user will import them via n8n UI: Settings → Import Workflow → paste JSON.

### n8n Workflow JSON Structure
An n8n workflow JSON looks like this (simplified):

```json
{
  "name": "Workflow Name",
  "nodes": [
    {
      "parameters": { ... },
      "id": "unique-uuid",
      "name": "Node Name",
      "type": "n8n-nodes-base.manualTrigger",
      "typeVersion": 1,
      "position": [250, 300]
    }
  ],
  "connections": {
    "Node Name": {
      "main": [[{ "node": "Next Node Name", "type": "main", "index": 0 }]]
    }
  },
  "active": false,
  "settings": { "executionOrder": "v1" }
}
```

### Key node types to use:
- Manual trigger: `n8n-nodes-base.manualTrigger`
- Schedule trigger: `n8n-nodes-base.scheduleTrigger`
- HTTP Request: `n8n-nodes-base.httpRequest`
- Code: `n8n-nodes-base.code`
- Google BigQuery: `n8n-nodes-base.googleBigQuery`

### Important n8n-specific details:
- The HTTP Request node for ClickHouse should use `sendBody: true` with `bodyContentType: raw` and the SQL query as the body text.
- The BigQuery node should use the `insert` operation, with `projectId: konvoai-n8n`, `datasetId: konvoai_analytics`, `tableId: customer_kpis_weekly`.
- For the BigQuery credential, reference the existing credential by name: `Google BigQuery Service Account`.
- For the ClickHouse credential, reference the existing HTTP Basic Auth credential.

---

## Validation Checklist

After creating both workflows:

1. [ ] Backfill workflow: Manual trigger → ClickHouse query (no date filter) → Code node (add ingested_at) → BigQuery insert
2. [ ] Weekly workflow: Schedule trigger (Monday 4AM CET) → ClickHouse query (previous week filter) → Code node (add ingested_at) → BigQuery insert
3. [ ] ClickHouse queries use `FORMAT JSONEachRow` (not `FORMAT JSON`)
4. [ ] ClickHouse queries are on a single line with no line breaks
5. [ ] All 15 KPI columns are present in both queries
6. [ ] BigQuery node uses Insert operation (not Execute Query)
7. [ ] BigQuery column names match exactly between ClickHouse output and BigQuery table
8. [ ] Weekly workflow schedule is set to Monday 4AM Europe/Berlin
9. [ ] Both workflows reference existing credentials from n8n credentials tab
10. [ ] Test backfill with a manual run and verify data appears in BigQuery

---

## Context

This is part of the KonvoAI customer analytics pipeline. KonvoAI is a B2B AI platform automating customer service for ~200 ecommerce companies. The pipeline extracts daily customer KPI snapshots from ClickHouse (the production database), aggregates them into weekly averages, and stores them in BigQuery for Looker Studio dashboards. The n8n instance at konvoai.app.n8n.cloud orchestrates this ETL process.
