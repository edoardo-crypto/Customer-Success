# Issue Classification Rules — Konvo AI

This document defines how customer-reported bugs are classified into categories
and subsections. It is the single source of truth for both human CS agents and
AI classifiers (n8n workflows + fetch_report_data.py).

---

## Categories (4)

### 1. AI Agent
Everything about how the AI assistant behaves, responds, and processes information.
If the AI did something wrong, didn't do something it should have, or an AI-related
platform feature is broken — it goes here.

**Subsections:**

| Subsection | What goes here | Examples |
|---|---|---|
| **Product recommendations** | AI recommending wrong, draft, or out-of-stock products. Stock data issues. Metafield reading failures. Product image recognition errors. | "AI recommended a draft product", "Incorrect stock info", "AI misidentified product from image" |
| **Order management** | AI can't find, retrieve, or process customer orders. Order status lookup failures. | "AI failed to retrieve order status", "Order management not working" |
| **Handover & transfers** | AI not transferring correctly to human agents. Double confirmations. Messages sent after transfer. Delayed handovers. | "AI asks for handoff confirmation twice", "AI sends handover instruction instead of following it" |
| **Language & formatting** | AI responding in wrong language, switching languages mid-message, sending formatting characters (\n\n). | "AI responds to English clients in Spanish", "AI sending /n/n formatting characters" |
| **OTP & verification** | AI-related OTP code failures. Verification flow issues where the AI is responsible. | "OTP code not identified by AI", "AI falsely promises SMS OTP" |
| **AI not responding** | AI is turned off, not activating, or silently not processing messages. Emails stuck in automated queue. | "AI not working despite being enabled", "AI was turned off", "Emails stuck in automated queue" |
| **AI response quality** | AI giving wrong information, not identifying itself, referencing wrong sources, misinterpreting customer intent. | "Why this reply sources not working", "AI failed to identify itself", "AI misinterpreted 'no'" |

**Key rule:** If a bug mentions "AI" but is about a platform feature being broken (e.g. inbox not loading, search bar), it does NOT go here — it goes in Inbox.

---

### 2. Inbox
Everything about the Konvo platform's internal features that are NOT AI-specific
and NOT about external integrations. This is about the platform's own UI, messaging
system, and inbox experience.

**Subsections:**

| Subsection | What goes here | Examples |
|---|---|---|
| **Speed & performance** | Inbox slow, pages not loading, messages delayed, requires manual refresh. | "Inbox speed issues", "Not loading messages without refresh", "Navigating inbox not smooth" |
| **Messages & conversations** | Messages missing, duplicated, expired, or in wrong state. Conversations merging incorrectly. Snooze reappearing. Side conversations broken. Assignment status not updating. | "Email address leaked into another conversation", "Multiple conversations open simultaneously", "Contact responses not appearing" |
| **Notifications** | Red dot not updating, push notifications not sent, notification badges stuck. | "Red dot notification does not update", "Notifications not being sent out" |
| **Other UI glitches** | Search bar issues, visual bugs (snooze button), display name mismatches, command input glitches, any other platform UI issue. | "Search bar does not accept spaces", "Name in Feed differs from conversation", "Snooze button visual bug" |

**Key rules:**
- If a bug is about data not appearing from an external tool (Shopify orders, Klaviyo data, Gorgias messages) → that's **Integration**, not Inbox
- If a bug is about files/attachments from WhatsApp or email not showing → that's **Integration** (channel issue)
- Flows are NEVER Inbox — they go to **WhatsApp Marketing**

---

### 3. WhatsApp Marketing
Everything about broadcasts (marketing campaigns) and flows (automated conversation
sequences). If a broadcast didn't send or a flow stopped mid-execution — it goes here.

**Subsections:**

| Subsection | What goes here | Examples |
|---|---|---|
| **Broadcasts not sending** | Broadcast errors, stuck in pending, variables failing, media file too large, CSV import issues, campaign cost not showing, scheduled broadcasts failing. | "Broadcast returning error status", "Variables showing errors", "Broadcast stuck in pending template" |
| **Flows stopping / misfiring** | Flows stopping mid-execution, triggering multiple times, not activating on trigger words, sending to wrong person, infinite loops, opt-out flows failing. | "Flow stopped in the middle", "Flow triggers not activating", "Opt out flow did not activate" |

**Key rule:** ALL flow and broadcast bugs go here — no exceptions. Even if the flow involves AI behavior (like an AI opt-out flow), the bug is about the flow mechanism failing, so it's WhatsApp Marketing.

---

### 4. Integration
Everything about connections between Konvo and external tools/channels. If data
isn't syncing or a channel isn't working — it goes here.

**Subsections:**

| Subsection | What goes here | Examples |
|---|---|---|
| **Helpdesk** | Gorgias, Zendesk integration issues. Can't send files, can't recommend products, messages not syncing through helpdesk. | "WhatsApp messages not syncing to Gorgias", "Cannot recommend products over Gorgias" |
| **Shopify / product sync** | Shopify product data not syncing, WooCommerce orders broken, product images not updating. | "Client orders not getting synced", "Product sync update needed for images" |
| **Channels (Email, Instagram, WhatsApp)** | Connection issues with any messaging channel. Attachments from Microsoft/Outlook not appearing. WhatsApp audio not transcribing. Instagram connection failures. Files sent via WhatsApp not showing in Konvo. Webhook failures for channel messages. | "Attachments from Microsoft not appearing", "WhatsApp audio transcripts stopped", "Not receiving messages in Konvo" |
| **CRM data sync** | Klaviyo data not syncing. Customer profile data missing. CRM properties not updating. Customer orders not appearing in Konvo profile. | "Klaviyo not synching", "Customer orders do not appear in profile" |

**Key rules:**
- If files/attachments from an external channel don't appear in Konvo → Integration > Channels (not Inbox)
- If customer data from Shopify/Klaviyo/CRM doesn't show in Konvo → Integration > CRM data sync (not Inbox)
- If Gorgias/Zendesk functionality is limited → Integration > Helpdesk (not Inbox)
- OTP/SMS delivery failures from the provider → Integration > Channels (not AI Agent)

---

## Decision Flowchart

```
Is it about a FLOW or BROADCAST?
  → YES → WhatsApp Marketing
  → NO ↓

Is it about an EXTERNAL TOOL not working with Konvo?
(Shopify, Gorgias, Zendesk, Klaviyo, Outlook, WhatsApp channel, Instagram)
  → YES → Integration
  → NO ↓

Is it about the AI's behavior, responses, or an AI-specific feature?
(product recs, order lookup, handover, OTP, AI not responding, AI quality)
  → YES → AI Agent
  → NO ↓

Is it about the Konvo platform's own UI/inbox/messaging?
  → YES → Inbox
```

---

## Disambiguation Rules (edge cases)

| Scenario | Correct category | Why |
|---|---|---|
| "AI not working" but the whole platform is down | Inbox > Speed & performance | Not AI-specific |
| Flow sent to wrong person | WhatsApp Marketing > Flows | ALL flows → WA Marketing |
| Opt-out flow didn't trigger | WhatsApp Marketing > Flows | ALL flows → WA Marketing |
| File from WhatsApp not appearing in Konvo | Integration > Channels | External channel data delivery |
| Klaviyo properties not syncing | Integration > CRM data sync | External tool sync |
| Customer orders not in Konvo profile | Integration > CRM data sync | Data from external source |
| Can't recommend products via Gorgias | Integration > Helpdesk | Helpdesk integration limit |
| OTP codes expiring immediately (SMS provider) | Integration > Channels | SMS channel/provider issue |
| OTP code not identified by AI | AI Agent > OTP & verification | AI failed to process the code |
| AI responds in wrong language | AI Agent > Language & formatting | AI behavior issue |
| Search bar doesn't work in inbox | Inbox > Other UI glitches | Platform UI |
| Search bar doesn't work in broadcast section | Inbox > Other UI glitches | Platform UI |
| "Playground to test AI not working" | AI Agent > AI response quality | AI-specific platform feature |
| Inbox slow / not loading | Inbox > Speed & performance | Platform performance |
| Broadcast stuck in pending | WhatsApp Marketing > Broadcasts | Campaign sending issue |
| Product data not syncing from Shopify | Integration > Shopify/product sync | External data sync |
| AI misidentified product from image | AI Agent > Product recommendations | AI vision/recommendation |
