import urllib.request
import json
import creds

token = creds.get("NOTION_TOKEN")
db_id = '84feda19cfaf4c6e9500bf21d2aaafef'
ds_id = '3ceb1ad0-91f1-40db-945a-c51c58035898'
block_id = '364a9bd6-413d-429b-b50b-9656cce4312a'

# Property IDs (URL-encoded as returned by API)
cs_sentiment_id = 'Bao%3B'      # Bao;
open_issues_id  = 'Q%5DRO'      # Q]RO

# New Health Status formula:
# 🔴 Red   if CS Sentiment == "At Risk"  OR  # of Open Issues > 5
# 🟡 Yellow if CS Sentiment == "Alright"
# 🟢 Green  otherwise
def prop_ref(prop_id):
    return f'{{{{notion:block_property:{prop_id}:{ds_id}:{block_id}}}}}'

sentiment = prop_ref(cs_sentiment_id)
open_issues = prop_ref(open_issues_id)

new_formula = (
    f'if(({sentiment} == "At Risk") or ({open_issues} > 5), '
    f'"🔴 Red", '
    f'if({sentiment} == "Alright", '
    f'"🟡 Yellow", '
    f'"🟢 Green"))'
)

print("New formula:")
print(new_formula)
print()

payload = {
    "properties": {
        # Update the Health Status formula
        "🚦 Health Status": {
            "formula": {
                "expression": new_formula
            }
        },
        # Delete Health Score by setting to null
        "❤️ Health Score": None
    }
}

data = json.dumps(payload).encode()

req = urllib.request.Request(
    f'https://api.notion.com/v1/data_sources/{ds_id}',
    data=data,
    method='PATCH',
    headers={
        'Authorization': f'Bearer {token}',
        'Notion-Version': '2025-09-03',
        'Content-Type': 'application/json'
    }
)

try:
    with urllib.request.urlopen(req) as response:
        result = json.load(response)
    print("✅ Success!")
    # Confirm the changes
    updated_props = result.get('properties', {})
    if '🚦 Health Status' in updated_props:
        print(f"  Health Status formula updated")
    if '❤️ Health Score' not in updated_props:
        print(f"  Health Score column deleted")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"❌ HTTP {e.code}: {body}")
