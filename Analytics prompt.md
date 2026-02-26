# Claude Code Prompt: Weekly Customer KPIs — ClickHouse to BigQuery

## Your first step
Read the file `Credentials.md` in your current working directory. It contains all credentials you need: the n8n API key and base URL, the ClickHouse host/username/password, and the BigQuery Service Account JSON. Do not proceed until you have read and parsed it.

---

## Goal
Build a new n8n workflow via the n8n REST API called **"Weekly Customer KPIs - ClickHouse to BigQuery"**.

This workflow has 4 nodes:
1. Schedule Trigger (every Monday 4AM UTC)
2. HTTP Request to query ClickHouse
3. Code node to parse the response, cast data types, and add a timestamp
4. BigQuery node to insert the rows

After creating the workflow, activate it and run a test execution.

---

## Step 0: Create the BigQuery credential in n8n

Before building the workflow, create the Google Service Account credential so the BigQuery node can authenticate.

1. Read the service account JSON from Credentials.md
2. Check if a credential named "Google BigQuery Service Account" already exists: `GET /api/v1/credentials`
3. If it does not exist, create it:
   ```
   POST /api/v1/credentials
   ```
   **Important:** The exact shape of the credential body depends on n8n's internal schema. First, inspect what credential types are available by checking any existing Google-related credentials in the GET response above. The `type` field is likely one of: `googleServiceAccount`, `googleApi`, or `googleBigQueryOAuth2Api`. You may need to experiment.

   A likely working body:
   ```json
   {
     "name": "Google BigQuery Service Account",
     "type": "googleServiceAccount",
     "data": {
       "email": "n8n-bigquery@konvoai-n8n.iam.gserviceaccount.com",
       "privateKey": "<private_key value from the service account JSON in Credentials.md, including the BEGIN/END markers>"
     }
   }
   ```
   If this returns an error, try `"type": "googleApi"` with the same data shape. Check the error message for hints about the expected type.

4. Save the returned credential `id` for use in Node 4.

---

## Step 1: Build the workflow JSON

### Node 1: Schedule Trigger
- Type: `n8n-nodes-base.scheduleTrigger`
- Trigger every Monday at 4:00 AM UTC
- This is the entry point of the workflow

### Node 2: HTTP Request — Query ClickHouse
- Type: `n8n-nodes-base.httpRequest`
- Method: `POST`
- URL: `https://eqgkgzq14k.eu-west-1.aws.clickhouse.cloud:8443/?database=operator&user=***REMOVED***&password=***REMOVED***`
  - Credentials are passed as query parameters (ClickHouse Cloud HTTP interface)
- Options: ClickHouse Cloud uses valid SSL certificates, so no special SSL config is needed
- Headers: none needed (default Content-Type is fine for plain text body)
- Body type: raw / string
- Body content (the exact SQL below, as a single string):

```sql
SELECT stripe_customer_id, org_id, workspace_id, toMonday(toDate(created_at)) AS week_start, avg(ai_resolution_rate) AS avg_ai_resolution_rate, avg(ai_sessions_total) AS avg_ai_sessions_total, avg(ai_sessions_resolved) AS avg_ai_sessions_resolved, avg(ai_sessions_unresolved) AS avg_ai_sessions_unresolved, avg(open_tickets_count) AS avg_open_tickets_count, avg(active_skills_count) AS avg_active_skills_count, avg(channels_with_ai_count) AS avg_channels_with_ai_count, avg(channels_connected_count) AS avg_channels_connected_count, avg(messages_sent24h) AS avg_messages_sent24h, avg(messages_received24h) AS avg_messages_received24h, count() AS data_points FROM operator.public_workspace_report_snapshot WHERE toDate(created_at) >= toMonday(today() - 7) AND toDate(created_at) < toMonday(today()) GROUP BY stripe_customer_id, org_id, workspace_id, week_start FORMAT JSON
```

> Keep the SQL on one line in the request body to avoid newline encoding issues.

### Node 3: Code — Parse and Transform
- Type: `n8n-nodes-base.code`
- Language: JavaScript
- This node does three things: parse the ClickHouse response, cast numeric types, and add `ingested_at`

Use this exact code:
```javascript
const body = $input.first().json;

// ClickHouse FORMAT JSON returns { data: [...], rows: N, meta: [...] }
// However, depending on how n8n parses the HTTP response, the structure
// might be directly in .json or nested. Handle both cases:
const data = body.data || body;

if (!Array.isArray(data) || data.length === 0) {
  throw new Error('ClickHouse returned no data for the previous week (Monday to Sunday).');
}

const now = new Date().toISOString();

return data.map(row => ({
  json: {
    week_start: String(row.week_start),
    stripe_customer_id: String(row.stripe_customer_id),
    org_id: String(row.org_id),
    workspace_id: String(row.workspace_id),
    avg_ai_resolution_rate: parseFloat(row.avg_ai_resolution_rate) || 0,
    avg_ai_sessions_total: parseFloat(row.avg_ai_sessions_total) || 0,
    avg_ai_sessions_resolved: parseFloat(row.avg_ai_sessions_resolved) || 0,
    avg_ai_sessions_unresolved: parseFloat(row.avg_ai_sessions_unresolved) || 0,
    avg_open_tickets_count: parseFloat(row.avg_open_tickets_count) || 0,
    avg_active_skills_count: parseFloat(row.avg_active_skills_count) || 0,
    avg_channels_with_ai_count: parseFloat(row.avg_channels_with_ai_count) || 0,
    avg_channels_connected_count: parseFloat(row.avg_channels_connected_count) || 0,
    avg_messages_sent24h: parseFloat(row.avg_messages_sent24h) || 0,
    avg_messages_received24h: parseFloat(row.avg_messages_received24h) || 0,
    data_points: parseInt(row.data_points, 10) || 0,
    ingested_at: now
  }
}));
```

### Node 4: BigQuery — Insert Rows
- Type: `n8n-nodes-base.googleBigQuery`
- Credential: reference the credential ID from Step 0
- Version: use the latest version of the node (v2 if available, check by setting `typeVersion: 2`)
- Operation: `insert`
- Project ID: `konvoai-n8n`
- Dataset: `konvoai_analytics`
- Table: `customer_kpis_weekly`
- Columns: map every field from the incoming items to the same-named BigQuery column. The field names in the Code node output match the BigQuery column names exactly, so use auto-mapping or explicit 1:1 mapping:
  - `week_start` → `week_start`
  - `stripe_customer_id` → `stripe_customer_id`
  - `org_id` → `org_id`
  - `workspace_id` → `workspace_id`
  - `avg_ai_resolution_rate` → `avg_ai_resolution_rate`
  - `avg_ai_sessions_total` → `avg_ai_sessions_total`
  - `avg_ai_sessions_resolved` → `avg_ai_sessions_resolved`
  - `avg_ai_sessions_unresolved` → `avg_ai_sessions_unresolved`
  - `avg_open_tickets_count` → `avg_open_tickets_count`
  - `avg_active_skills_count` → `avg_active_skills_count`
  - `avg_channels_with_ai_count` → `avg_channels_with_ai_count`
  - `avg_channels_connected_count` → `avg_channels_connected_count`
  - `avg_messages_sent24h` → `avg_messages_sent24h`
  - `avg_messages_received24h` → `avg_messages_received24h`
  - `data_points` → `data_points`
  - `ingested_at` → `ingested_at`

### Node connections
In the workflow JSON `connections` object, wire them in sequence:
- Schedule Trigger → HTTP Request (ClickHouse)
- HTTP Request → Code (Parse and Transform)
- Code → BigQuery (Insert)

---

## Step 2: Create the workflow via the n8n API

1. First check if a workflow named "Weekly Customer KPIs - ClickHouse to BigQuery" already exists:
   ```
   GET /api/v1/workflows
   ```
   Search the response for a matching name. If found, use that workflow's `id` and skip to Step 3.

2. If it does not exist, create it:
   ```
   POST /api/v1/workflows
   ```
   Send the full workflow JSON including `name`, `nodes` (array of 4 node objects), `connections`, and `settings`.

   **Critical details about the workflow JSON format:**
   - Each node needs: `id` (UUID), `name` (display name), `type`, `typeVersion`, `position` (array of [x, y]), and `parameters`
   - The Schedule Trigger node needs `typeVersion: 1.1` or `1.2`
   - The HTTP Request node needs `typeVersion: 4.2` (latest)
   - The Code node needs `typeVersion: 2`
   - The BigQuery node needs `typeVersion: 2` and a `credentials` object like:
     ```json
     "credentials": {
       "googleBigQueryOAuth2Api": {
         "id": "<credential_id_from_step_0>",
         "name": "Google BigQuery Service Account"
       }
     }
     ```
   - The `connections` object format is:
     ```json
     {
       "Schedule Trigger": {
         "main": [[{ "node": "Query ClickHouse", "type": "main", "index": 0 }]]
       },
       "Query ClickHouse": {
         "main": [[{ "node": "Parse and Transform", "type": "main", "index": 0 }]]
       },
       "Parse and Transform": {
         "main": [[{ "node": "Insert to BigQuery", "type": "main", "index": 0 }]]
       }
     }
     ```
     The keys must match the `name` field of each node exactly.

3. If the POST returns an error, read the error message carefully. Common issues:
   - Invalid node type version: try decrementing the `typeVersion`
   - Invalid credential type: check Step 0 and adjust
   - Missing required fields: add them based on the error message

---

## Step 3: Activate and test

1. Activate the workflow:
   ```
   PATCH /api/v1/workflows/{id}
   Content-Type: application/json

   { "active": true }
   ```

2. Run a test execution:
   ```
   POST /api/v1/workflows/{id}/run
   ```
   This returns an `executionId`.

3. Poll for completion:
   ```
   GET /api/v1/executions/{executionId}
   ```
   Poll every 5 seconds, up to 60 seconds max. Wait until `status` is not `"running"`.

4. If the execution succeeded, report the results. If it failed, read the error from the execution data, fix the issue in the workflow JSON, update the workflow via `PATCH /api/v1/workflows/{id}`, and re-run the test. Repeat up to 3 times.

---

## Debugging tips

- If the ClickHouse HTTP request returns an error, check: is the URL correct? Are the user/password query params URL-encoded if they contain special characters? The host is `eqgkgzq14k.eu-west-1.aws.clickhouse.cloud` on port `8443`.
- If the Code node fails, the error will be in `executionData.resultData.runData["Parse and Transform"]`. Read the error message.
- If BigQuery insert fails with a 403, the credential type or permissions are wrong. Try re-creating the credential with a different `type` value.
- If BigQuery insert fails with "invalid rows", it is likely a data type mismatch. Check that `week_start` is a string in `YYYY-MM-DD` format and that numeric fields are actual numbers, not strings.
- You can inspect any node's output in the execution data at `executionData.resultData.runData["<node name>"]`.

---

## Final output
After completing, tell me:
1. The workflow ID and a direct link to it in n8n (format: `https://konvoai.app.n8n.cloud/workflow/<id>`)
2. Whether the test execution succeeded (include the execution ID)
3. How many rows were returned from ClickHouse
4. How many rows were inserted into BigQuery
5. Any errors encountered and how they were resolved
