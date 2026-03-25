import urllib.request
import json
import creds

token = creds.get("NOTION_TOKEN")
ds_id = '3ceb1ad0-91f1-40db-945a-c51c58035898'

all_results = []
start_cursor = None

while True:
    body = {'page_size': 100}
    if start_cursor:
        body['start_cursor'] = start_cursor

    req = urllib.request.Request(
        f'https://api.notion.com/v1/data_sources/{ds_id}/query',
        data=json.dumps(body).encode(),
        headers={
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2025-09-03',
            'Content-Type': 'application/json'
        }
    )
    with urllib.request.urlopen(req) as response:
        data = json.load(response)

    all_results.extend(data['results'])
    if not data.get('has_more'):
        break
    start_cursor = data['next_cursor']

print(f'Total customers: {len(all_results)}')
print()

mismatches = []
all_rows = []

for r in all_results:
    props = r['properties']

    name_prop = props.get('🏢 Company Name', {})
    name = name_prop.get('title', [{}])
    company = name[0].get('plain_text', '(no name)') if name else '(no name)'

    hs_prop = props.get('❤️ Health Score', {})
    health_score = (hs_prop.get('select') or {}).get('name', None)

    hst_prop = props.get('🚦 Health Status', {})
    health_status = (hst_prop.get('formula') or {}).get('string', None)

    all_rows.append((company, health_score, health_status))

    hs_is_red = bool(health_score and 'Red' in health_score)
    hst_is_red = bool(health_status and 'Red' in health_status)
    hs_is_orange = bool(health_score and 'Orange' in health_score)
    hst_is_yellow = bool(health_status and 'Yellow' in health_status)
    hs_is_green = bool(health_score and 'Green' in health_score)
    hst_is_green = bool(health_status and 'Green' in health_status)

    if hs_is_red and not hst_is_red:
        mismatches.append((company, health_score, health_status, 'Score=RED but Status is not Red'))
    elif not hs_is_red and hst_is_red:
        mismatches.append((company, health_score, health_status, 'Status=RED but Score is not Red'))
    elif hs_is_green and not hst_is_green:
        mismatches.append((company, health_score, health_status, 'Score=GREEN but Status is not Green'))
    elif not hs_is_green and hst_is_green and health_score is not None:
        mismatches.append((company, health_score, health_status, 'Status=GREEN but Score is not Green'))

mismatch_companies = {m[0] for m in mismatches}

print(f'{"Company":<45} {"Health Score":<22} {"Health Status":<20}')
print('-' * 90)
for company, hs, hst in sorted(all_rows):
    flag = '  ⚠️' if company in mismatch_companies else ''
    print(f'{company:<45} {str(hs):<22} {str(hst):<20}{flag}')

print()
print(f'=== MISMATCHES ({len(mismatches)}) ===')
if mismatches:
    for company, hs, hst, reason in mismatches:
        print(f'  ⚠️  {company}')
        print(f'      Health Score:  {hs}')
        print(f'      Health Status: {hst}')
        print(f'      Issue: {reason}')
        print()
else:
    print('  None — all consistent!')
