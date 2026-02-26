# Claude Code Prompt: Weekly Customer KPIs - ClickHouse to BigQuery

## Your first step
Read the file `Credentials.md` in your current working directory. It contains all credentials you need: the n8n API key and base URL, the ClickHouse host/username/password, and the BigQuery Service Account JSON. Do not proceed until you have read it.

---

## Goal
Build a new n8n workflow via the n8n REST API called **"Weekly Customer KPIs - ClickHouse to BigQuery"**.

This workflow must:
- Trigger every Monday at 4AM
- Query ClickHouse for the previous Monday-to-Sunday week of data
- Aggregate all KPIs per customer (one row per customer per week)
- Insert the results into a BigQuery table
- Skip any rows where that `stripe_customer_id` + `week_start` combination already exists (no duplicates, no overwrites)
- Include customers with zero activity for the week (include them with zero values, do not exclude them)
- Be activated automatically after creation
- Run a test execution after creation and report the result

---

## n8n Workflow Structure

### Node 1: Schedule Trigger
- Type: `n8n-nodes-base.scheduleTrigger`
- Runs every Monday at 4:00 AM

### Node 2: HTTP Request - Query ClickHouse
- Type: `n8n-nodes-base.httpRequest`
- Method: POST
- URL: ClickHouse host from Credentials.md (port 8443)
- Authentication: Basic Auth (username + password from Credentials.md)
- Headers: `Content-Type: text/plain`
- Body: the following SQL query as plain text:

```sql
SELECT
    stripe_customer_id,
    org_id,
    workspace_id,
    toMonday(toDate(created_at)) AS week_start,
    avg(ai_resolution_rate) AS avg_ai_resolution_rate,
    avg(ai_sessions_total) AS avg_ai_sessions_total,
    avg(ai_sessions_resolved) AS avg_ai_sessions_resolved,
    avg(ai_sessions_unresolved) AS avg_ai_sessions_unresolved,
    avg(open_tickets_count) AS avg_open_tickets_count,
    avg(active_skills_count) AS avg_active_skills_count,
    avg(channels_with_ai_count) AS avg_channels_with_ai_count,
    avg(channels_connected_count) AS avg_channels_connected_count,
    avg(messages_sent24h) AS avg_messages_sent24h,
    avg(messages_received24h) AS avg_messages_received24h,
    count() AS data_points
FROM operator.public_workspace_report_snapshot
WHERE toDate(created_at) >= toMonday(today() - 7)
  AND toDate(created_at) < toMonday(today())
GROUP BY stripe_customer_id, org_id, workspace_id, week_start
FORMAT JSON
```

### Node 3: Code - Transform + Deduplicate
- Type: `n8n-nodes-base.code`
- Parse the ClickHouse response body (it returns `{ data: [...], meta: [...] }`)
- If `data` is empty, throw an error with message "ClickHouse returned no data for this week"
- For each row in `data`, add `ingested_at` as the current UTC ISO timestamp
- Then query BigQuery (via the BigQuery REST API using the service account JWT) to check which `stripe_customer_id + week_start` combinations already exist for this week
- Filter out any rows that already exist in BigQuery
- Output the remaining rows as an array of n8n items

To authenticate with BigQuery REST API from the Code node, generate a JWT from the service account JSON (which you will read from Credentials.md and make available as an environment variable or hardcode into the Code node). Use the JWT to call the BigQuery REST API directly.

### Node 4: BigQuery - Insert Rows
- Type: `n8n-nodes-base.googleBigQuery` (use the native n8n BigQuery node)
- Credential: Google Service Account using the JSON from Credentials.md
- Operation: Insert rows
- Project ID: `konvoai-n8n`
- Dataset: `konvoai_analytics`
- Table: `customer_kpis_weekly`
- Map each field from the transformed data to the corresponding BigQuery column

---

## BigQuery Table Schema (already created, do not recreate)
| Column | Type |
|---|---|
| week_start | DATE REQUIRED |
| stripe_customer_id | STRING |
| org_id | STRING |
| workspace_id | STRING |
| avg_ai_resolution_rate | FLOAT |
| avg_ai_sessions_total | FLOAT |
| avg_ai_sessions_resolved | FLOAT |
| avg_ai_sessions_unresolved | FLOAT |
| avg_open_tickets_count | FLOAT |
| avg_active_skills_count | FLOAT |
| avg_channels_with_ai_count | FLOAT |
| avg_channels_connected_count | FLOAT |
| avg_messages_sent24h | FLOAT |
| avg_messages_received24h | FLOAT |
| data_points | INTEGER |
| ingested_at | TIMESTAMP |

---

## How to create the workflow via the n8n API
1. Authenticate all requests with the n8n API key from Credentials.md in the header: `X-N8N-API-KEY: <key>`
2. Base URL: the n8n base URL from Credentials.md
3. Create the workflow: `POST /api/v1/workflows` with the full workflow JSON
4. Activate it: `PATCH /api/v1/workflows/{id}` with `{ "active": true }`
5. Run a test execution: `POST /api/v1/workflows/{id}/run`
6. Poll `GET /api/v1/executions/{executionId}` until status is not `running`, then report success or failure with the full output

---

## Error handling requirements
- If ClickHouse returns no rows: stop execution and log clearly
- If BigQuery insert fails: log the error with the specific rows that failed
- If the workflow already exists with the same name: skip creation and just activate + test it

---

## Final output
After completing, tell me:
1. The workflow ID and a direct link to it in n8n
2. Whether the test execution succeeded
3. How many rows were inserted into BigQuery
4. Any errors encountered and how they were resolved
