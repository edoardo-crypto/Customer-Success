# Konvoai Documentation: Gap Analysis & Migration Plan

**Date:** 2026-02-23 (revised)
**Old docs:** https://docs.konvoai.com/en/ — 88 articles (WhatsApp marketing era)
**New docs:** Intercom Help Center — modular section structure

---

## Summary

| Category | Count |
|---|---|
| Articles in new sections (Sections 1–4 + Integrations, after migration) | 42 |
| Gaps resolved by moving existing articles | 5 |
| New articles written from scratch | 0 |
| **Total after reorganization** | **42** |

> All 5 "gap" articles already existed in Intercom (legacy collections or orphaned).
> They were moved into the correct new sections. No articles were written from scratch.

---

## Revised Gap Analysis: Existing Articles Moved

| # | Gap Title | Intercom ID | Old Home | Action Taken | New Location |
|---|---|---|---|---|---|
| 1 | Message Composer | 9474341 | Collection 9613647 | Renamed + moved | 3.4: Section 3 Inbox |
| 2 | Quick Replies | 9538772 | Collection 9613647 | Renamed + moved | 3.5: Section 3 Inbox |
| 3 | Snooze Conversations | 11537314 | Orphaned (no collection) | Renamed + moved | 3.6: Section 3 Inbox |
| 4 | Open a New Conversation | 10301960 | Orphaned (no collection) | **Refreshed** (channel-agnostic) + moved | 3.7: Section 3 Inbox |
| 5 | Enable Notifications | 9685054 | Collection 9593962 | Renamed + moved | 1.4: Section 1 Overview |

**Script used:** `reorganize_articles.py`

---

## Old vs New Docs: Full Comparison

| Old Article | New Equivalent | Status |
|---|---|---|
| Getting started / platform overview | "Introduction to Konvoai" | ✅ covered |
| AI Agent setup | "Configure Your AI Agent" | ✅ covered |
| Knowledge Hub | "Knowledge Hub" | ✅ covered |
| Training the AI | "Train Your AI with Conversations" | ✅ covered |
| AI Inbox | "AI Inbox" | ✅ covered |
| Dashboard / Analytics | "AI Dashboard" | ✅ covered |
| Contacts | "Contacts" | ✅ covered |
| Settings overview | "Settings" articles | ✅ covered |
| WhatsApp connection | Integrations > WhatsApp | ✅ migrated |
| Gmail connection | Integrations > Gmail | ✅ migrated |
| Meta / Instagram | Integrations > Meta / Instagram | ✅ migrated |
| Live Chat widget | Integrations > Live Chat | ✅ migrated |
| Generic Email (IMAP/SMTP) | Integrations > Generic Email | ✅ migrated |
| Side Conversation Email | Integrations > Side Conversation Email | ✅ migrated |
| Shopify integration | Integrations > Shopify | ✅ migrated |
| WooCommerce integration | Integrations > WooCommerce | ✅ migrated |
| Klaviyo integration | Integrations > Klaviyo | ✅ migrated |
| Appstle (subscriptions) | Integrations > Appstle | ✅ migrated |
| Loop (subscriptions) | Integrations > Loop | ✅ migrated |
| Recharge (subscriptions) | Integrations > Recharge | ✅ migrated |
| SendCloud (shipping) | Integrations > SendCloud | ✅ migrated |
| Gorgias (helpdesk) | Integrations > Gorgias | ✅ migrated |
| Zendesk (helpdesk) | Integrations > Zendesk | ✅ migrated |
| Message composer (file limits, audio, templates) | 3.4 Message Composer | ✅ moved (was in old collection) |
| Quick Replies / "/" shortcut | 3.5 Quick Replies | ✅ moved (was in old collection) |
| Snooze Conversations | 3.6 Snooze Conversations | ✅ moved (was orphaned) |
| Open a New Conversation (outbound) | 3.7 Open a New Conversation | ✅ refreshed + moved (was orphaned) |
| Enable Notifications | 1.4 Enable Notifications | ✅ moved (was in old collection) |
| Invite Team & First Setup | — | ⏭ deferred (not critical for launch) |
| Troubleshooting & FAQ | — | ⏭ deferred |
| GDPR & Opt-In Compliance | — | ⏭ deferred |
| WhatsApp setup via 360Dialog | — | 🗑 obsolete |
| Flows automation builder (12 articles) | — | 🗑 obsolete |
| WhatsApp Broadcasts / Campaigns (3) | — | 🗑 obsolete |
| Segments & Audience management (3) | — | 🗑 obsolete |
| Revenue Attribution (old) | — | 🗑 obsolete |
| Bring customers to WhatsApp (2) | — | 🗑 obsolete |
| Flow templates (6) | — | 🗑 obsolete |
| WhatsApp tiers & spam limits | — | 🗑 obsolete |
| WhatsApp conversation costs | — | 🗑 obsolete |
| Marketing Messages API | — | 🗑 obsolete |
| Klaviyo pop-ups for WhatsApp | — | 🗑 obsolete |

---

## Final Help Center Structure (42 articles)

```
1: Konvo AI Overview (4 articles)
   1.1 Welcome to Konvo AI
   1.2 Use Cases
   1.3 Quick Start Guide
   1.4 Enable Notifications           ← moved from legacy collection 9593962

2: AI Configuration Deep Dive (9 articles)
   2.0–2.8 (AI config — unchanged)

3: Inbox (8 articles)
   3.0 Inbox Overview
   3.1 Conversation Feed
   3.2 Agents Configuration
   3.3 Shared Views
   3.4 Message Composer               ← moved from legacy collection 9613647
   3.5 Quick Replies                  ← moved from legacy collection 9613647
   3.6 Snooze Conversations           ← moved (was orphaned)
   3.7 Open a New Conversation        ← refreshed (channel-agnostic) + moved (was orphaned)

4: Settings (11 articles)
   4.0–4.10 (unchanged)

5: Integrations (15 articles)
   5.1  WhatsApp
   5.2  Gmail
   5.3  Meta / Instagram
   5.4  Live Chat
   5.5  Generic Email
   5.6  Side Conversation Email
   5.7  Shopify
   5.8  WooCommerce
   5.9  Klaviyo
   5.10 Appstle
   5.11 Loop
   5.12 Recharge
   5.13 SendCloud
   5.14 Gorgias
   5.15 Zendesk
```

**Total: 42 articles** (reorganization only — no new articles written)

---

## Content Refresh: Open a New Conversation (10301960)

The original body contained WhatsApp-specific language ("enter their WhatsApp number").
The refreshed version covers all channels generically:
- How to find/select a contact
- How to pick a channel (WhatsApp, email, live chat, etc.)
- Channel-specific notes (WhatsApp opt-in requirement, email free-form)
- Troubleshooting pointer to Settings → Integrations

---

## Phase 3: Safely Deletable Old Docs (~38 articles)

These are all from the old WhatsApp marketing product and should be archived/deleted.

| Category | Article Count | Reason |
|---|---|---|
| WhatsApp setup via 360Dialog | 7 | Old connection method |
| Flows automation builder | 12 | Not in new product |
| WhatsApp Broadcasts / Campaigns | 3 | Replaced by AI inbox model |
| Segments & Audience management | 3 | Not in new product |
| Revenue Attribution (old) | 1 | Replaced by AI Dashboard |
| Bring customers to WhatsApp | 2 | Old marketing use case |
| Most Used Flow templates | 6 | Templates for old flows |
| WhatsApp tiers & spam limits | 1 | Not relevant to AI service |
| WhatsApp conversation costs | 1 | Old billing model |
| Marketing Messages API | 1 | Old WhatsApp marketing API |
| Klaviyo pop-ups for WhatsApp | 1 | Old opt-in pop-up feature |
| **Total** | **~38** | |
