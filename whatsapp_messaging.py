"""WhatsApp Cloud API Text Messaging

Sends post-call follow-up messages to callers via WhatsApp Cloud API.
This is separate from Pipecat (which only handles voice).

Uses the 24-hour messaging window: since the user just called us,
we are within the window and can send session messages without templates.
"""

import json
import os

import aiohttp
from loguru import logger
from openai import AsyncOpenAI

from knowledge import load_prompt

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v21.0")
SUPPORT_PHONE = os.getenv("IFS_SUPPORT_PHONE", "+91 78913 93505")

WHATSAPP_API_URL = (
    f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{WHATSAPP_PHONE_NUMBER_ID}/messages"
)


async def send_whatsapp_text(to_phone: str, message: str) -> bool:
    """Send a text message via WhatsApp Cloud API.

    Args:
        to_phone: Recipient phone number (E.164 format without +, e.g. '919876543210')
        message: Text message body (max 4096 chars)

    Returns:
        True if message was sent successfully, False otherwise.
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        logger.warning("WhatsApp credentials not configured, skipping text message")
        return False

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_phone,
        "type": "text",
        "text": {
            "body": message[:4096],
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WHATSAPP_API_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info(f"WhatsApp text sent to {to_phone}")
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"WhatsApp API returned {resp.status}: {body}")
                    return False
    except Exception as e:
        logger.error(f"Failed to send WhatsApp text to {to_phone}: {e}")
        return False


async def mark_message_as_read(message_id: str) -> bool:
    """Send read receipt (blue ticks) for a WhatsApp message.

    Args:
        message_id: The wamid of the incoming message.

    Returns:
        True if read receipt was sent successfully.
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        return False

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "status": "read",
        "message_id": message_id,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WHATSAPP_API_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.debug(f"Read receipt sent for {message_id}")
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"Read receipt failed {resp.status}: {body}")
                    return False
    except Exception as e:
        logger.error(f"Failed to send read receipt: {e}")
        return False


async def send_whatsapp_template(
    to_phone: str,
    template_name: str,
    language: str = "en",
    components: list | None = None,
) -> dict:
    """Send a template message via WhatsApp Cloud API.

    Template messages can be sent outside the 24-hour messaging window.
    Templates must be pre-approved in Meta Business Manager.

    Args:
        to_phone: Recipient phone number (E.164 without +)
        template_name: Approved template name (e.g. 'hello_world')
        language: Template language code (default 'en')
        components: Optional template components (header, body, button params)

    Returns:
        {"success": True, "wa_message_id": "wamid.xxx"} on success,
        {"success": False, "error": "..."} on failure.
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        logger.warning("WhatsApp credentials not configured, skipping template")
        return {"success": False, "error": "WhatsApp credentials not configured"}

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    template_obj = {
        "name": template_name,
        "language": {"code": language},
    }
    if components:
        template_obj["components"] = components

    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_phone,
        "type": "template",
        "template": template_obj,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WHATSAPP_API_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    wa_message_id = ""
                    messages = data.get("messages", [])
                    if messages:
                        wa_message_id = messages[0].get("id", "")
                    logger.info(f"Template '{template_name}' sent to {to_phone} (wamid: {wa_message_id})")
                    return {"success": True, "wa_message_id": wa_message_id}
                else:
                    body = await resp.text()
                    logger.warning(f"Template send failed {resp.status}: {body}")
                    return {"success": False, "error": body[:200]}
    except Exception as e:
        logger.error(f"Failed to send template to {to_phone}: {e}")
        return {"success": False, "error": str(e)[:200]}


async def get_whatsapp_templates() -> list[dict]:
    """Fetch message templates from Meta Graph API with full details.

    First resolves the WABA ID from the phone number, then fetches templates
    using the WABA endpoint which returns richer data (category, components, etc.).
    Falls back to phone-number-level endpoint if WABA lookup fails.

    Returns list of template dicts with: name, status, language, category, components
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        return []

    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    waba_id = os.getenv("WHATSAPP_BUSINESS_ACCOUNT_ID", "")

    # Try to resolve WABA ID from phone number if not configured
    if not waba_id:
        try:
            async with aiohttp.ClientSession() as session:
                phone_url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{WHATSAPP_PHONE_NUMBER_ID}?fields=whatsapp_business_account"
                async with session.get(phone_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        waba_id = data.get("whatsapp_business_account", {}).get("id", "")
        except Exception as e:
            logger.warning(f"Failed to resolve WABA ID: {e}")

    # Use WABA endpoint if we have the ID, otherwise fall back
    if waba_id:
        base_url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{waba_id}/message_templates"
    else:
        base_url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{WHATSAPP_PHONE_NUMBER_ID}/message_templates"

    all_templates = []
    url = f"{base_url}?limit=100&fields=name,status,language,category,components"

    try:
        async with aiohttp.ClientSession() as session:
            while url:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"Template fetch failed {resp.status}: {body}")
                        break
                    data = await resp.json()
                    all_templates.extend(data.get("data", []))
                    # Follow pagination cursor
                    url = data.get("paging", {}).get("next", "")
    except Exception as e:
        logger.error(f"Failed to fetch templates: {e}")
        return all_templates

    logger.info(f"Fetched {len(all_templates)} WhatsApp templates")
    return all_templates


async def send_interactive_message(
    to_phone: str,
    interactive_type: str,
    body_text: str,
    buttons: list[dict] | None = None,
    sections: list[dict] | None = None,
    header: dict | None = None,
    footer: str | None = None,
) -> dict:
    """Send an interactive message (buttons or list) via WhatsApp Cloud API.

    Args:
        to_phone: Recipient phone number (E.164 without +)
        interactive_type: 'button' for reply buttons, 'list' for list menu
        body_text: Main message body text
        buttons: For type='button': list of {"id": "btn_1", "title": "Click me"}
        sections: For type='list': list of {"title": "Section", "rows": [{"id": "row_1", "title": "Option", "description": "..."}]}
        header: Optional header dict {"type": "text", "text": "Header text"}
        footer: Optional footer text

    Returns:
        {"success": True, "wa_message_id": "..."} or {"success": False, "error": "..."}
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        return {"success": False, "error": "WhatsApp credentials not configured"}

    hdrs = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    interactive = {
        "type": interactive_type,
        "body": {"text": body_text[:1024]},
    }

    if header:
        interactive["header"] = header
    if footer:
        interactive["footer"] = {"text": footer[:60]}

    if interactive_type == "button" and buttons:
        interactive["action"] = {
            "buttons": [
                {"type": "reply", "reply": {"id": b["id"], "title": b["title"][:20]}}
                for b in buttons[:3]  # Max 3 buttons
            ]
        }
    elif interactive_type == "list" and sections:
        interactive["action"] = {
            "button": "View Options",
            "sections": sections[:10],  # Max 10 sections
        }

    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_phone,
        "type": "interactive",
        "interactive": interactive,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WHATSAPP_API_URL,
                json=payload,
                headers=hdrs,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    wa_message_id = ""
                    messages = data.get("messages", [])
                    if messages:
                        wa_message_id = messages[0].get("id", "")
                    logger.info(f"Interactive '{interactive_type}' sent to {to_phone}")
                    return {"success": True, "wa_message_id": wa_message_id}
                else:
                    body = await resp.text()
                    logger.warning(f"Interactive send failed {resp.status}: {body}")
                    return {"success": False, "error": body[:200]}
    except Exception as e:
        logger.error(f"Failed to send interactive to {to_phone}: {e}")
        return {"success": False, "error": str(e)[:200]}


async def send_followup_message(
    caller_phone: str,
    caller_name: str,
    handoff_requested: bool,
    transcript: list[dict] | None = None,
    topics: list[str] | None = None,
    knowledge_context: str = "",
):
    """Send personalized post-call follow-up message via GPT-4o.

    Uses the call transcript and knowledge base to generate a relevant
    follow-up message instead of a generic thank-you.

    Falls back to a generic message if GPT-4o is unavailable or fails.
    """
    if not caller_phone:
        return

    name = caller_name or "there"

    # Try GPT-4o personalized message if we have a transcript
    if transcript and os.getenv("OPENAI_API_KEY"):
        personalized = await _generate_personalized_followup(
            name, handoff_requested, transcript, topics or [], knowledge_context
        )
        if personalized:
            await send_whatsapp_text(caller_phone, personalized)
            return

    # Fallback: generic message
    if handoff_requested:
        message = (
            f"Hi {name}! Thank you for calling Institute of Financial Studies.\n\n"
            f"We noticed you would like to speak with our team directly. "
            f"A team member will reach out to you shortly.\n\n"
            f"In the meantime, feel free to reach us at:\n"
            f"Phone: {SUPPORT_PHONE}\n"
            f"Mon-Sat: 10 AM - 6 PM\n\n"
            f"Thank you for your interest in IFS!"
        )
    else:
        message = (
            f"Hi {name}! Thank you for calling Institute of Financial Studies.\n\n"
            f"If you have any more questions, feel free to call us again or reach out at:\n"
            f"Phone: {SUPPORT_PHONE}\n"
            f"Mon-Sat: 10 AM - 6 PM\n\n"
            f"We look forward to hearing from you!"
        )

    await send_whatsapp_text(caller_phone, message)


async def _generate_personalized_followup(
    caller_name: str,
    handoff_requested: bool,
    transcript: list[dict],
    topics: list[str],
    knowledge_context: str,
) -> str | None:
    """Use GPT-4o to generate a personalized follow-up from the call transcript."""
    # Format transcript for the prompt
    transcript_text = "\n".join(
        f"{t['role'].upper()}: {t['content']}" for t in transcript if t.get("content")
    )

    followup_prompt = load_prompt("followup", "")
    if not followup_prompt:
        logger.warning("No followup prompt template found, using generic message")
        return None

    try:
        system_prompt = followup_prompt.format(
            knowledge=knowledge_context or "No knowledge available.",
            transcript=transcript_text,
            caller_name=caller_name,
            topics=", ".join(topics) if topics else "General inquiry",
            handoff="Yes" if handoff_requested else "No",
            support_phone=SUPPORT_PHONE,
        )
    except KeyError as e:
        logger.error(f"Followup prompt template missing placeholder: {e}")
        return None

    try:
        client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": system_prompt}],
            max_tokens=400,
            temperature=0.7,
        )
        message = response.choices[0].message.content.strip()
        logger.info(f"Personalized follow-up generated: {message[:100]}...")
        return message
    except Exception as e:
        logger.error(f"GPT-4o follow-up generation failed: {e}")
        return None
