"""
Migrate Notion documentation to Intercom Help Center articles.
Usage: python3 migrate.py
"""

import os
import re
import time
import html
import json
import requests

# ─── Constants ────────────────────────────────────────────────────────────────

PARENT_PAGE_ID = "2ffe418fd8c4806bb095c0e21bb9c6eb"
NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
INTERCOM_API_BASE = "https://api.intercom.io"
INTERCOM_VERSION = "2.11"
CREDENTIALS_FILE = "credentials.md"


# ─── Credentials ──────────────────────────────────────────────────────────────

def read_credentials():
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CREDENTIALS_FILE)
    with open(creds_path, 'r') as f:
        content = f.read()
    notion = re.search(r'## Notion\n```\n(.+?)\n```', content, re.DOTALL).group(1).strip()
    intercom = re.search(r'## Intercom\n```\n(.+?)\n```', content, re.DOTALL).group(1).strip()
    return {'notion': notion, 'intercom': intercom}


# ─── HTTP helper with retry ────────────────────────────────────────────────────

def api_request(method, url, headers, json_body=None, retries=3):
    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, headers=headers, json=json_body, timeout=30)
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Network error on {method} {url}: {e}")

        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"    [rate limit] sleeping {wait}s...")
            time.sleep(wait)
            continue

        if resp.status_code >= 500:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Server error {resp.status_code} on {method} {url}: {resp.text[:200]}")

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code} on {method} {url}: {resp.text[:300]}")

        return resp.json()

    raise RuntimeError(f"Failed after {retries} retries: {method} {url}")


# ─── Notion API ───────────────────────────────────────────────────────────────

def notion_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def get_notion_blocks(block_id, notion_token):
    """Paginated fetch of all direct children of a block/page."""
    headers = notion_headers(notion_token)
    results = []
    cursor = None
    while True:
        url = f"{NOTION_API_BASE}/blocks/{block_id}/children?page_size=100"
        if cursor:
            url += f"&start_cursor={cursor}"
        data = api_request("GET", url, headers)
        results.extend(data.get('results', []))
        if not data.get('has_more'):
            break
        cursor = data['next_cursor']
    return results


# ─── Rich text → HTML ─────────────────────────────────────────────────────────

def rich_text_to_html(rich_text_array):
    """Convert a Notion rich_text array to an HTML string."""
    parts = []
    for rt in rich_text_array:
        text = rt.get('plain_text', '')
        # HTML-escape first, then convert soft returns → <br>
        # (must escape before inserting raw HTML tags)
        text = html.escape(text, quote=False)
        text = text.replace('\n', '<br>')

        ann = rt.get('annotations', {})
        # Apply annotations innermost first (code wraps everything)
        if ann.get('code'):
            text = f'<code>{text}</code>'
        if ann.get('bold'):
            text = f'<b>{text}</b>'
        if ann.get('italic'):
            text = f'<i>{text}</i>'
        if ann.get('strikethrough'):
            text = f'<s>{text}</s>'
        if ann.get('underline'):
            text = f'<u>{text}</u>'

        # Apply link — skip Notion internal links
        link = rt.get('text', {}).get('link')
        if link and link.get('url') and not link['url'].startswith('https://www.notion.so'):
            text = f'<a href="{html.escape(link["url"])}">{text}</a>'

        parts.append(text)
    return ''.join(parts)


# ─── Block renderers ──────────────────────────────────────────────────────────

def render_list_item(block, notion_token):
    """Render a single list item's text + any nested list children."""
    block_type = block['type']
    text = rich_text_to_html(block[block_type]['rich_text'])
    if block.get('has_children'):
        children = get_notion_blocks(block['id'], notion_token)
        text += blocks_to_html(children, notion_token)
    return text


def render_single_block(block, notion_token):
    """Render any non-list block type to HTML."""
    t = block['type']

    if t in ('heading_1', 'heading_2'):
        text = rich_text_to_html(block[t]['rich_text'])
        return f'<h2>{text}</h2>'

    elif t == 'heading_3':
        text = rich_text_to_html(block[t]['rich_text'])
        return f'<h3>{text}</h3>'

    elif t == 'paragraph':
        text = rich_text_to_html(block['paragraph']['rich_text'])
        if text.strip():
            return f'<p>{text}</p>'
        else:
            return '<p>&nbsp;</p>'

    elif t == 'divider':
        return '<hr>'

    elif t == 'callout':
        icon_obj = block['callout'].get('icon', {})
        if icon_obj.get('type') == 'emoji':
            icon = icon_obj.get('emoji', '')
        else:
            icon = ''
        text = rich_text_to_html(block['callout']['rich_text'])
        return (
            f'<div style="background:#f5f5f5;padding:12px;border-radius:6px;margin:8px 0;">'
            f'{icon} {text}</div>'
        )

    elif t == 'quote':
        text = rich_text_to_html(block['quote']['rich_text'])
        return f'<blockquote>{text}</blockquote>'

    elif t == 'code':
        # Use plain_text directly — no annotation escaping
        code_blocks = block['code'].get('rich_text', [])
        plain = ''.join(rt.get('plain_text', '') for rt in code_blocks)
        plain_escaped = html.escape(plain)
        return f'<pre><code>{plain_escaped}</code></pre>'

    elif t == 'toggle':
        text = rich_text_to_html(block['toggle']['rich_text'])
        result = f'<p><b>{text}</b></p>'
        if block.get('has_children'):
            children = get_notion_blocks(block['id'], notion_token)
            result += blocks_to_html(children, notion_token)
        return result

    elif t == 'table':
        rows = get_notion_blocks(block['id'], notion_token)
        has_header = block['table'].get('has_column_header', False)
        html_out = '<table style="border-collapse:collapse;width:100%">'
        for idx, row in enumerate(rows):
            cells = row['table_row']['cells']  # list of rich_text arrays
            tag = 'th' if (idx == 0 and has_header) else 'td'
            style = 'border:1px solid #ddd;padding:8px;'
            html_out += '<tr>' + ''.join(
                f'<{tag} style="{style}">{rich_text_to_html(cell)}</{tag}>'
                for cell in cells
            ) + '</tr>'
        html_out += '</table>'
        return html_out

    elif t == 'image':
        img = block['image']
        url = img.get('external', {}).get('url') or img.get('file', {}).get('url', '')
        return f'<img src="{html.escape(url)}" alt="image" style="max-width:100%">'

    elif t == 'child_page':
        title = block['child_page'].get('title', 'Untitled')
        return f'<p>→ <em>{html.escape(title)}</em></p>'

    elif t == 'column_list':
        if not block.get('has_children'):
            return ''
        columns = get_notion_blocks(block['id'], notion_token)
        result = ''
        for col in columns:
            if col.get('has_children'):
                col_blocks = get_notion_blocks(col['id'], notion_token)
                result += blocks_to_html(col_blocks, notion_token)
        return result

    elif t == 'bookmark':
        url = block.get('bookmark', {}).get('url', '')
        return f'<p><a href="{html.escape(url)}">{html.escape(url)}</a></p>'

    elif t == 'link_preview':
        url = block.get('link_preview', {}).get('url', '')
        return f'<p><a href="{html.escape(url)}">{html.escape(url)}</a></p>'

    elif t == 'synced_block':
        if block.get('has_children'):
            children = get_notion_blocks(block['id'], notion_token)
            return blocks_to_html(children, notion_token)
        return ''

    else:
        return f'<!-- unsupported: {t} -->'


def blocks_to_html(blocks, notion_token):
    """
    Convert a flat list of Notion blocks to HTML.
    Uses index-walk to group consecutive list items into <ul>/<ol>.
    """
    html_out = ''
    i = 0
    while i < len(blocks):
        b = blocks[i]
        t = b['type']

        if t == 'bulleted_list_item':
            items = []
            while i < len(blocks) and blocks[i]['type'] == 'bulleted_list_item':
                items.append(blocks[i])
                i += 1
            html_out += '<ul>' + ''.join(
                f'<li>{render_list_item(it, notion_token)}</li>' for it in items
            ) + '</ul>'

        elif t == 'numbered_list_item':
            items = []
            while i < len(blocks) and blocks[i]['type'] == 'numbered_list_item':
                items.append(blocks[i])
                i += 1
            html_out += '<ol>' + ''.join(
                f'<li>{render_list_item(it, notion_token)}</li>' for it in items
            ) + '</ol>'

        else:
            html_out += render_single_block(b, notion_token)
            i += 1

    return html_out


def fetch_article_html(page_id, notion_token):
    """Fetch all blocks of a page and convert to HTML."""
    blocks = get_notion_blocks(page_id, notion_token)
    return blocks_to_html(blocks, notion_token)


# ─── Structure parsing ────────────────────────────────────────────────────────

def parse_sections(parent_blocks):
    """
    Walk parent page blocks:
    - heading_2 → start a new section
    - child_page → add article to current section
    Returns list of {'name': str, 'articles': [{'page_id', 'title'}]}
    """
    sections = []
    current = None

    for block in parent_blocks:
        t = block['type']

        if t == 'heading_2':
            name = ''.join(rt['plain_text'] for rt in block['heading_2']['rich_text'])
            current = {'name': name, 'articles': []}
            sections.append(current)

        elif t == 'child_page' and current is not None:
            title = block['child_page'].get('title', 'Untitled')
            current['articles'].append({
                'page_id': block['id'],
                'title': title,
            })

    return sections


# ─── Intercom API ─────────────────────────────────────────────────────────────

def intercom_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Intercom-Version": INTERCOM_VERSION,
        "Content-Type": "application/json",
    }


def get_intercom_admin_id(intercom_token):
    """Get the author ID for Edoardo Pelli, falling back to first admin."""
    data = api_request("GET", f"{INTERCOM_API_BASE}/admins", intercom_headers(intercom_token))
    admins = data.get('admins', [])
    for admin in admins:
        if admin.get('name') == 'Edoardo Pelli':
            return admin['id']
    if admins:
        return admins[0]['id']
    raise RuntimeError("No admins found in Intercom")


def create_intercom_collection(name, intercom_token):
    """Create a Help Center collection and return its ID."""
    data = api_request(
        "POST",
        f"{INTERCOM_API_BASE}/help_center/collections",
        intercom_headers(intercom_token),
        json_body={"name": name},
    )
    return data['id']


def create_intercom_article(title, body, collection_id, admin_id, intercom_token):
    """Create a draft article in the given collection and return its ID."""
    clean_title = re.sub(r'^Article\s+', '', title)  # "Article 2.1: Knowledge Hub" → "2.1: Knowledge Hub"
    data = api_request(
        "POST",
        f"{INTERCOM_API_BASE}/articles",
        intercom_headers(intercom_token),
        json_body={
            "title": clean_title,
            "description": clean_title,
            "body": body,
            "author_id": admin_id,
            "state": "draft",
            "parent_id": collection_id,
            "parent_type": "collection",
        },
    )
    return data['id']


# ─── Main flow ────────────────────────────────────────────────────────────────

def main():
    creds = read_credentials()
    print("Credentials loaded.")

    # Step 1: Parse Notion structure
    print(f"\nFetching parent page structure (ID: {PARENT_PAGE_ID})...")
    parent_blocks = get_notion_blocks(PARENT_PAGE_ID, creds['notion'])
    sections = parse_sections(parent_blocks)

    total_articles = sum(len(s['articles']) for s in sections)
    print(f"Found {len(sections)} sections, {total_articles} articles:")
    for s in sections:
        print(f"  {s['name']} → {len(s['articles'])} articles")

    # Step 2: Get Intercom author
    print("\nFetching Intercom admin ID...")
    admin_id = get_intercom_admin_id(creds['intercom'])
    print(f"Using admin ID: {admin_id}")

    # Step 3: Create collections + articles
    results = []
    for section in sections:
        print(f"\n{'─'*60}")
        print(f"Creating collection: {section['name']}")
        collection_id = create_intercom_collection(section['name'], creds['intercom'])
        print(f"  Collection ID: {collection_id}")
        time.sleep(0.5)

        for article in section['articles']:
            print(f"  Fetching: {article['title']}...")
            try:
                body = fetch_article_html(article['page_id'], creds['notion'])
                time.sleep(0.3)  # Notion rate limit courtesy
                print(f"  Creating article in Intercom ({len(body)} chars HTML)...")
                article_id = create_intercom_article(
                    article['title'], body, collection_id, admin_id, creds['intercom']
                )
                time.sleep(0.5)
                results.append({
                    'section': section['name'],
                    'title': article['title'],
                    'id': article_id,
                    'status': 'OK',
                })
                print(f"  ✓ Created: {article['title']} (id: {article_id})")
            except Exception as e:
                results.append({
                    'section': section['name'],
                    'title': article['title'],
                    'id': None,
                    'status': f'FAILED: {e}',
                })
                print(f"  ✗ FAILED: {article['title']}: {e}")

    # Step 4: Summary
    print("\n\n=== MIGRATION SUMMARY ===")
    print(f"{'Section':<38} {'Title':<42} {'ID':<12} {'Status'}")
    print("─" * 110)
    for r in results:
        section_short = r['section'][:37]
        title_short = r['title'][:41]
        print(f"{section_short:<38} {title_short:<42} {str(r['id']):<12} {r['status']}")

    ok = sum(1 for r in results if r['status'] == 'OK')
    print(f"\n{ok}/{len(results)} articles created successfully.")


if __name__ == '__main__':
    main()
