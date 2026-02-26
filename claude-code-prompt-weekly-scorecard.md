# Claude Code Prompt: Build Weekly CS Scorecard (Notion DB + First Week Data)

## Context

KonvoAI is a B2B AI customer service platform with ~180 customers managed by two CS managers (Alex and Aya). We are building a weekly accountability system: a Notion database that stores one row per week with 6 KPIs tracked per CS manager.

This is Phase 1 (manual). You will:
1. Create the Notion database
2. Query the Master Customer Table to compute this week's KPI values
3. Write the first scorecard row

## Notion Environment

**Parent page (CS Ops Hub):** `302e418fd8c4818e9235ff950f55a31b`

The new database should be created as a child of this page.

**Master Customer Table:**
- Database ID: `84feda19cfaf4c6e9500bf21d2aaafef`
- Data source: `collection://3ceb1ad0-91f1-40db-945a-c51c58035898`

## Master Customer Table Schema (relevant fields only)

These are the exact field names with emoji prefixes as they exist in Notion:

| Field | Type | Values / Notes |
|---|---|---|
| `🏢 Company Name` | title | |
| `⭐ CS Owner` | select | "Alex", "Aya" |
| `❤️ Journey Stage` | formula | Outputs text (e.g., "Churned", "Pre-launch", "Launched", "Value Confirmed", "Nurturing", "At Risk", "Pending Review") |
| `🚦 Health Status` | formula | Outputs text (e.g., "Red", "Yellow", "Green") |
| `🧠 CS Sentiment` | select | "Great", "Alright", "At Risk", "Not a customer" |
| `📞 Days Since Last Contact` | formula | Outputs a number |
| `📞 Last Contact Date` | date | |
| `😢 Churn Date` | date | |
| `🚀 Graduation Date` | date | |
| `⚠️ # of Open Issues` | rollup | Number |
| `💰 MRR` | number | Euro format |
| `💰 Plan Tier` | select | "Start", "Scale", "All In", "Custom", "Unknown" |

**Important:** `❤️ Journey Stage` and `🚦 Health Status` are formulas, not selects. You cannot filter on them using select operators. You will need to query all rows and filter programmatically, or use the Notion API's formula filter if supported.

## Task 1: Create "Weekly CS Scorecards" Database

Create a new Notion database as a child of the CS Ops Hub page (`302e418fd8c4818e9235ff950f55a31b`).

**Database title:** `📊 Weekly CS Scorecards`

**Schema:**

| Field | Type | Description |
|---|---|---|
| Week | Title | Format: "W09 (Feb 24 - Mar 2)" |
| Week Start | Date | Monday of the week |
| Alex: Red Health | Number | KPI 1 for Alex |
| Aya: Red Health | Number | KPI 1 for Aya |
| Alex: No Contact >21d | Number | KPI 2 for Alex |
| Aya: No Contact >21d | Number | KPI 2 for Aya |
| Alex: Avg Reply Time | Number | KPI 3 for Alex (minutes). Leave empty for now, manual entry. |
| Aya: Avg Reply Time | Number | KPI 3 for Aya (minutes). Leave empty for now, manual entry. |
| Alex: Churned | Number | KPI 4 for Alex |
| Aya: Churned | Number | KPI 4 for Aya |
| Alex: Graduated | Number | KPI 5 for Alex |
| Aya: Graduated | Number | KPI 5 for Aya |
| Alex: Customers Contacted | Number | KPI 6 for Alex. Leave empty for now, manual entry. |
| Aya: Customers Contacted | Number | KPI 6 for Aya. Leave empty for now, manual entry. |
| Notes | Text (rich text) | Free text for context, decisions, follow-ups |

No relations, no formulas, no rollups. Just simple fields.

## Task 2: Compute W09 KPI Values

**Current week (W09):** Monday Feb 24 to Sunday Mar 2, 2026.

Query the Master Customer Table and compute the following. For each KPI, compute separately for `⭐ CS Owner` = "Alex" and `⭐ CS Owner` = "Aya".

### KPI 1: Customers at Red Health
Count rows where:
- `🚦 Health Status` (formula output) = "Red"
- `❤️ Journey Stage` (formula output) ≠ "Churned"
- Grouped by `⭐ CS Owner`

### KPI 2: Customers Not Contacted >21 Days
Count rows where:
- `📞 Days Since Last Contact` (formula output) > 21
- `❤️ Journey Stage` (formula output) ≠ "Churned"
- Grouped by `⭐ CS Owner`

### KPI 4: Customers Churned This Week
Count rows where:
- `😢 Churn Date` falls between 2026-02-24 and 2026-03-02 (inclusive)
- Grouped by `⭐ CS Owner`

### KPI 5: Customers Graduated This Week
Count rows where:
- `🚀 Graduation Date` falls between 2026-02-24 and 2026-03-02 (inclusive)
- Grouped by `⭐ CS Owner`

### KPI 3 and KPI 6: Skip (manual entry)
These require Intercom API and Google Calendar data respectively. Leave these fields empty in the scorecard row. They will be filled manually before the Friday meeting.

## Task 3: Write the First Scorecard Row

Create a single page in the new database with:
- **Week:** "W09 (Feb 24 - Mar 2)"
- **Week Start:** 2026-02-24
- All computed KPI values from Task 2
- KPI 3 and KPI 6 fields left empty
- **Notes:** "First scorecard. KPI 3 (reply time) and KPI 6 (customers contacted) to be filled manually from Intercom and calendar data."

## Implementation Notes

### Querying formula fields
Notion's API allows filtering on formula fields using the `formula` filter type. For example, to filter `🚦 Health Status` = "Red", use a formula filter with `string.equals = "Red"`. However, if this doesn't work reliably via MCP tools, the fallback is:
1. Query all rows from the Master Table (paginate if needed, there are ~180 rows)
2. For each row, read the formula field values
3. Filter and count programmatically

### Approach
Use the Notion MCP tools available in your environment:
- `notion-create-database` to create the scorecard DB
- `notion-search` or `notion-fetch` with `collection://3ceb1ad0-91f1-40db-945a-c51c58035898` to query the Master Table
- `notion-create-pages` to write the scorecard row

If the MCP query tools don't support formula field filtering, use the `query_data_sources` tool or fetch all rows and process them in code.

### Output
After completing all three tasks, report:
1. The URL of the new Weekly CS Scorecards database
2. The computed KPI values (show the breakdown)
3. The URL of the first scorecard row
