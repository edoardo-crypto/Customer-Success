"""
creds.py — Shared credential loader for all CS scripts.

Usage:
    import creds
    NOTION_TOKEN = creds.get("NOTION_TOKEN")

Looks up credentials in this order:
  1. Environment variable (used in CI / GitHub Actions)
  2. Credentials.md (used in local development — gitignored)

Supported names:
  NOTION_TOKEN, STRIPE_KEY, HUBSPOT_TOKEN, LINEAR_TOKEN,
  INTERCOM_TOKEN, SLACK_BOT_TOKEN, SLACK_WEBHOOK_CS,
  ANTHROPIC_API_KEY, N8N_API_KEY
"""

import os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CREDS_FILE = os.path.join(_SCRIPT_DIR, "Credentials.md")

# Maps env-var name → (Credentials.md section header, optional line prefix filter)
_MAP = {
    "NOTION_TOKEN":     ("Notion",           None),
    "STRIPE_KEY":       ("Stripe",           None),
    "HUBSPOT_TOKEN":    ("HubSpot",          None),
    "LINEAR_TOKEN":     ("Linear",           "lin_api_"),
    "INTERCOM_TOKEN":   ("Intercom",         None),
    "SLACK_BOT_TOKEN":  ("Slack",            "xoxb-"),
    "SLACK_WEBHOOK_CS": ("Slack",            "https://hooks.slack.com"),
    "ANTHROPIC_API_KEY": ("Anthropic (Claude)", None),
    "N8N_API_KEY":      ("n8n",              "eyJ"),
    "SLA_SLACK_CHANNEL": ("Slack",           "C"),
}

_cache = {}


def _extract_blocks(header):
    """Return all ```-fenced code blocks under ## {header} in Credentials.md."""
    blocks = []
    try:
        raw = open(_CREDS_FILE).read()
        start = raw.index(f"## {header}")
        # Stop at the next ## heading (or end of file)
        next_section = raw.find("\n## ", start + 1)
        section = raw[start:next_section] if next_section != -1 else raw[start:]
        pos = 0
        while True:
            bs = section.find("```", pos)
            if bs == -1:
                break
            bs += 3
            # skip optional language tag (e.g. ```json)
            if bs < len(section) and section[bs] not in ("\n", "\r"):
                bs = section.index("\n", bs) + 1
            be = section.find("```", bs)
            if be == -1:
                break
            blocks.append(section[bs:be].strip())
            pos = be + 3
    except (FileNotFoundError, ValueError):
        pass
    return blocks


def get(name):
    """Return credential value for the given name. Raises RuntimeError if not found."""
    if name in _cache:
        return _cache[name]

    # 1. Try environment variable
    val = os.environ.get(name, "").strip()
    if val:
        _cache[name] = val
        return val

    # 2. Try Credentials.md
    if name not in _MAP:
        raise RuntimeError(f"Unknown credential: {name}")

    header, prefix = _MAP[name]
    blocks = _extract_blocks(header)
    for block in blocks:
        if prefix:
            for line in block.splitlines():
                line = line.strip()
                if line.startswith(prefix):
                    _cache[name] = line
                    return line
        else:
            if block:
                _cache[name] = block
                return block

    raise RuntimeError(
        f"{name} not found. Set it as an env var or add it to Credentials.md "
        f"under ## {header}"
    )
