# Claude Code Prompt: Weekly Customer KPIs — ClickHouse to BigQuery

## Your first step
Read the file `Credentials.md` in your current working directory. It contains all credentials you need: the n8n API key and base URL, the ClickHouse host/username/password, and the BigQuery Service Account JSON. Do not proceed until you have read and parsed it.

---

## Goal
Build a new n8n workflow via the n8n REST API called **"Weekly Customer KPIs - ClickHouse to BigQuery"**.

This workflow must:
- Trigger every Monday at 4:00 AM UTC
- Query ClickHouse for the previous Monday-to-Sunday week of data
- Aggregate all KPIs per customer (one row per customer per week)
- Insert the results into a BigQuery table
- Skip any rows where that `stripe_customer_id` + `week_start` combination already exists (no duplicates, no overwrites)
- Be activated automatically after creation
- Run a test execution after creation and report the result

> **Note on zero-activity customers:** The snapshot table `operator.public_workspace_report_snapshot` contains daily snapshots for all active workspaces. If a customer has no row in the snapshot for a given week, they are genuinely inactive and should NOT be included. Only customers present in the snapshot table are in scope.

---

## Prerequisites: Create the Google Service Account Credential in n8n

Before building the workflow, you must create the BigQuery credential in n8n so the native BigQuery node can use it.

1. Read the BigQuery Service Account JSON from `Credentials.md`
2. Create the credential via the n8n API:
   ```
   POST /api/v1/credentials
   ```
   With this body:
   ```json
   {
     "name": "Google BigQuery Service Account",
     "type": "googleApi",
     "data": {
       "email": "<client_email from service account JSON>",
       "privateKey": "<private_key from service account JSON>",
       "impersonateUser": ""
     }
   }
   ```
3. Save the returned credential `id`. You will reference it in Node 6 (BigQuery Insert).

If a credential with the name "Google BigQuery Service Account" already exists, skip creation and use the existing one. You can check with `GET /api/v1/credentials` and filter by name.

---

## n8n Workflow Structure (7 nodes)

### Node 1: Schedule Trigger
- Type: `n8n-nodes-base.scheduleTrigger`
- Runs every Monday at 4:00 AM UTC
- Connects to: Node 2

### Node 2: HTTP Request — Query ClickHouse
- Type: `n8n-nodes-base.httpRequest`
- Method: POST
- URL: `https://<CLICKHOUSE_HOST from Credentials.md>:8443/?database=operator`
- Authentication: Use n8n's built-in "Generic Credential Type" > "Basic Auth" with the ClickHouse username and password from Credentials.md. Alternatively, you can pass the credentials as query parameters: `&user=<USER>&password=<PASS>` appended to the URL.
- SSL: Set `"allowUnauthorizedCerts": true` in the node options (ClickHouse may use a self-signed certificate)
- Headers: `Content-Type: text/plain`
- Response format: Set to JSON (the response will be parsed automatically)
- Body (raw/text): the SQL query below, sent as the request body

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

- Connects to: Node 3

### Node 3: Code — Parse ClickHouse Response
- Type: `n8n-nodes-base.code`
- Purpose: Parse the ClickHouse JSON response and add the `ingested_at` timestamp
- Logic:
  1. Read the response body from the previous node. ClickHouse `FORMAT JSON` returns `{ "data": [...], "rows": N, "meta": [...] }`.
  2. Extract the `data` array. If it is empty or missing, throw an error: `"ClickHouse returned no data for the previous week (Monday to Sunday)."`
  3. For each row in `data`, add a field `ingested_at` set to `new Date().toISOString()`
  4. Also ensure `week_start` is formatted as `YYYY-MM-DD` (ClickHouse returns it this way by default, but verify)
  5. Output each row as a separate n8n item

Example code structure:
```javascript
const response = $input.first().json;
const data = response.data || [];

if (data.length === 0) {
  throw new Error('ClickHouse returned no data for the previous week (Monday to Sunday).');
}

const now = new Date().toISOString();

return data.map(row => ({
  json: {
    ...row,
    ingested_at: now
  }
}));
```

- Connects to: Node 4

### Node 4: HTTP Request — Check Existing Rows in BigQuery
- Type: `n8n-nodes-base.httpRequest`
- Purpose: Query BigQuery to find which `stripe_customer_id + week_start` combinations already exist, so we can skip them
- Method: POST
- URL: `https://bigquery.googleapis.com/bigquery/v2/projects/konvoai-n8n/queries`
- Authentication: Use n8n's built-in "Google Service Account" or "OAuth2" credential (the same one created in Prerequisites). Alternatively, use "Predefined Credential Type" > "Google BigQuery OAuth2 API" if available.

  **Important:** If built-in credential auth does not work cleanly for an HTTP Request node pointing at the BigQuery REST API, use the following approach instead: make this a second Code node that uses the service account to generate an access token via Google's OAuth2 token endpoint, then calls the BigQuery query API. See the fallback approach at the bottom of this section.

- Body:
```json
{
  "query": "SELECT DISTINCT stripe_customer_id, CAST(week_start AS STRING) AS week_start FROM `konvoai-n8n.konvoai_analytics.customer_kpis_weekly` WHERE week_start = DATE(toMonday_result_from_clickhouse)",
  "useLegacySql": false
}
```

  Since the `week_start` value is the same for all rows in a given execution (it's always the previous Monday), extract it from the first item and inject it into the query:
  ```
  WHERE week_start = DATE('2026-02-09')
  ```

- Connects to: Node 5

**Fallback approach (if credential-based auth doesn't work for HTTP Request to BigQuery):**
Make Node 4 a Code node instead. Inside it:
1. Hardcode the service account JSON (or read it from the workflow's static data)
2. Create a JWT with `iss` = client_email, `scope` = `https://www.googleapis.com/auth/bigquery`, `aud` = `https://oauth2.googleapis.com/token`, `iat` = now, `exp` = now + 3600
3. Sign it with the private key using RS256 (use n8n's built-in `crypto` module or the `jsonwebtoken` library if available)
4. POST to `https://oauth2.googleapis.com/token` with `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=<signed_jwt>`
5. Use the returned `access_token` to call the BigQuery query API
6. Return the list of existing `stripe_customer_id` values

**If this is too complex or the `jsonwebtoken` library is not available in n8n's Code node sandbox, use this simpler alternative:** Skip the BigQuery dedup check entirely and instead modify the ClickHouse SQL in Node 2 to be idempotent by design. Since this runs weekly and BigQuery's `insertAll` API does not enforce uniqueness, add a note that duplicate prevention is handled at the query level (the same week's data will produce the same aggregated values, so re-inserting is harmless). Then remove Nodes 4 and 5 entirely, and connect Node 3 directly to Node 6.

### Node 5: Code — Filter Out Duplicates
- Type: `n8n-nodes-base.code`
- Purpose: Compare the ClickHouse rows against the BigQuery existing rows and filter out duplicates
- Logic:
  1. Read the BigQuery response from Node 4. The response contains `rows` with `stripe_customer_id` and `week_start` values.
  2. Build a Set of existing keys: `"stripe_customer_id|week_start"`
  3. Read all items from Node 3 (the ClickHouse rows) using `$items("Code — Parse ClickHouse Response")`
  4. Filter: keep only rows whose key is NOT in the existing set
  5. If zero rows remain after filtering, return an empty array (this is NOT an error, it means all data was already ingested)

Example code structure:
```javascript
// Get BigQuery response (from Node 4)
const bqResponse = $input.first().json;
const existingRows = bqResponse?.rows || [];

const existingKeys = new Set(
  existingRows.map(r => `${r.f[0].v}|${r.f[1].v}`)
);

// Get all ClickHouse items (passed through from Node 3)
// Note: you may need to adjust how items flow between nodes.
// If Node 4 doesn't pass through the ClickHouse items, 
// use $items() or $node["Node 3"].json to reference them.
const clickhouseItems = $items("Code — Parse ClickHouse Response");

const newRows = clickhouseItems.filter(item => {
  const key = `${item.json.stripe_customer_id}|${item.json.week_start}`;
  return !existingKeys.has(key);
});

if (newRows.length === 0) {
  // Return empty to signal "nothing to insert"
  return [];
}

return newRows;
```

- Connects to: Node 6

### Node 6: IF — Check If There Are Rows to Insert
- Type: `n8n-nodes-base.if`
- Purpose: Only proceed to BigQuery insert if there are rows to insert. If Node 5 outputs zero items, skip the insert.
- Condition: Check if the number of input items > 0
- True branch: connects to Node 7
- False branch: connects to nothing (workflow ends cleanly)

### Node 7: BigQuery — Insert Rows
- Type: `n8n-nodes-base.googleBigQuery`
- Credential: Reference the credential ID created in the Prerequisites step (name: "Google BigQuery Service Account")
- Operation: Insert
- Project ID: `konvoai-n8n`
- Dataset: `konvoai_analytics`
- Table: `customer_kpis_weekly`
- Column mapping (map each incoming field to the corresponding BigQuery column):

| Incoming field | BigQuery column |
|---|---|
| week_start | week_start |
| stripe_customer_id | stripe_customer_id |
| org_id | org_id |
| workspace_id | workspace_id |
| avg_ai_resolution_rate | avg_ai_resolution_rate |
| avg_ai_sessions_total | avg_ai_sessions_total |
| avg_ai_sessions_resolved | avg_ai_sessions_resolved |
| avg_ai_sessions_unresolved | avg_ai_sessions_unresolved |
| avg_open_tickets_count | avg_open_tickets_count |
| avg_active_skills_count | avg_active_skills_count |
| avg_channels_with_ai_count | avg_channels_with_ai_count |
| avg_channels_connected_count | avg_channels_connected_count |
| avg_messages_sent24h | avg_messages_sent24h |
| avg_messages_received24h | avg_messages_received24h |
| data_points | data_points |
| ingested_at | ingested_at |

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
3. **Check if workflow already exists:** `GET /api/v1/workflows` and search for a workflow with the name "Weekly Customer KPIs - ClickHouse to BigQuery". If it exists, skip creation and use the existing workflow ID.
4. **Create the workflow:** `POST /api/v1/workflows` with the full workflow JSON (including all nodes, connections, and settings)
5. **Activate it:** `PATCH /api/v1/workflows/{id}` with `{ "active": true }`
6. **Run a test execution:** `POST /api/v1/workflows/{id}/run`
7. **Poll for completion:** `GET /api/v1/executions/{executionId}` every 5 seconds until `status` is not `"running"`. Maximum 60 seconds of polling. Then report success or failure with the full output.

---

## Error handling requirements

| Scenario | Behavior |
|---|---|
| ClickHouse returns no rows | Node 3 throws an error, workflow stops. Error message: "ClickHouse returned no data for the previous week." |
| BigQuery dedup check fails | Log the error, but continue with the insert (assume no duplicates exist). Do not block the entire workflow. |
| All rows already exist in BigQuery | Node 5 returns empty array, Node 6 skips the insert. Workflow ends successfully with a log: "All rows already existed in BigQuery, nothing to insert." |
| BigQuery insert fails | Log the error with the HTTP status code and response body. Include the count of rows that were attempted. |
| Workflow already exists with same name | Skip creation, use existing workflow ID. Activate and test it. |
| n8n credential creation fails | Log the error. Check if credential already exists and reuse it. |

---

## Important implementation notes

1. **Node connections:** Make sure the workflow JSON includes the `connections` object that links each node to the next in sequence: 1→2→3→4→5→6→7 (with Node 6 branching: true→7, false→end).

2. **Item passthrough:** The BigQuery dedup check (Node 4) receives items from Node 3 but needs to pass them through to Node 5. Ensure that Node 5 can access both the BigQuery response AND the original ClickHouse items. If n8n's data flow doesn't support this natively, merge the data in Node 5 by referencing `$node["Code — Parse ClickHouse Response"]` or storing items in workflow static data.

3. **Data types:** ClickHouse `FORMAT JSON` returns all numeric values as strings. The BigQuery node may need them as actual numbers. In Node 3, parse numeric fields with `parseFloat()` and `parseInt()` accordingly:
   - All `avg_*` fields: `parseFloat()`
   - `data_points`: `parseInt()`
   - `week_start`: keep as string in `YYYY-MM-DD` format

4. **Timezone:** The Schedule Trigger should be set to UTC. ClickHouse's `today()` function uses the server's timezone (usually UTC), which aligns correctly.

5. **Simplification option:** If the BigQuery dedup (Nodes 4-6) proves too complex to implement reliably, you may simplify by removing those nodes and connecting Node 3 directly to Node 7. In this case, add a comment in the workflow noting that duplicate prevention relies on not running the workflow more than once per week, and that re-runs for the same week will insert duplicate rows. This is acceptable for an MVP.

---

## Final output
After completing, tell me:
1. The workflow ID and a direct link to it in n8n
2. Whether the test execution succeeded or failed (include the execution ID)
3. How many rows were returned from ClickHouse
4. How many rows were inserted into BigQuery (after dedup filtering, if applicable)
5. Any errors encountered and how they were resolved
