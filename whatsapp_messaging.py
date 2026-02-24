"""WhatsApp Cloud API Text Messaging

Sends post-call follow-up messages to callers via WhatsApp Cloud API.
This is separate from Pipecat (which only handles voice).

Uses the 24-hour messaging window: since the user just called us,
we are within the window and can send session messages without templates.
"""

import os

import aiohttp
from loguru import logger

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v21.0")

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
) -> bool:
    """Send a template message via WhatsApp Cloud API.

    Template messages can be sent outside the 24-hour messaging window.
    Templates must be pre-approved in Meta Business Manager.

    Args:
        to_phone: Recipient phone number (E.164 without +)
        template_name: Approved template name (e.g. 'hello_world')
        language: Template language code (default 'en')
        components: Optional template components (header, body, button params)

    Returns:
        True if sent successfully, False otherwise.
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        logger.warning("WhatsApp credentials not configured, skipping template")
        return False

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
                    logger.info(f"Template '{template_name}' sent to {to_phone}")
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"Template send failed {resp.status}: {body}")
                    return False
    except Exception as e:
        logger.error(f"Failed to send template to {to_phone}: {e}")
        return False


async def send_interactive_buttons(
    to_phone: str,
    body_text: str,
    buttons: list[dict],
    header_text: str = "",
    footer_text: str = "",
) -> bool:
    """Send an interactive button message via WhatsApp Cloud API.

    Args:
        to_phone: Recipient phone number (E.164 without +)
        body_text: Main message body
        buttons: List of buttons, each: {"id": "btn_1", "title": "Click Me"}
                 Max 3 buttons, title max 20 chars
        header_text: Optional header text
        footer_text: Optional footer text

    Returns:
        True if sent successfully, False otherwise.
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        return False

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    action_buttons = [
        {"type": "reply", "reply": {"id": b["id"], "title": b["title"][:20]}}
        for b in buttons[:3]
    ]

    interactive = {
        "type": "button",
        "body": {"text": body_text[:1024]},
        "action": {"buttons": action_buttons},
    }
    if header_text:
        interactive["header"] = {"type": "text", "text": header_text[:60]}
    if footer_text:
        interactive["footer"] = {"text": footer_text[:60]}

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
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info(f"Interactive buttons sent to {to_phone}")
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"Interactive send failed {resp.status}: {body}")
                    return False
    except Exception as e:
        logger.error(f"Failed to send interactive to {to_phone}: {e}")
        return False


async def send_interactive_list(
    to_phone: str,
    body_text: str,
    button_text: str,
    sections: list[dict],
    header_text: str = "",
    footer_text: str = "",
) -> bool:
    """Send an interactive list message via WhatsApp Cloud API.

    Args:
        to_phone: Recipient phone number
        body_text: Main message body
        button_text: Text on the list button (max 20 chars)
        sections: List of sections, each:
                  {"title": "Section", "rows": [{"id": "1", "title": "Item", "description": "..."}]}
        header_text: Optional header
        footer_text: Optional footer

    Returns:
        True if sent successfully.
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        return False

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    interactive = {
        "type": "list",
        "body": {"text": body_text[:1024]},
        "action": {
            "button": button_text[:20],
            "sections": sections[:10],
        },
    }
    if header_text:
        interactive["header"] = {"type": "text", "text": header_text[:60]}
    if footer_text:
        interactive["footer"] = {"text": footer_text[:60]}

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
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info(f"Interactive list sent to {to_phone}")
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"List send failed {resp.status}: {body}")
                    return False
    except Exception as e:
        logger.error(f"Failed to send list to {to_phone}: {e}")
        return False


async def get_whatsapp_templates() -> list[dict]:
    """Fetch approved message templates from Meta Graph API.

    Returns list of template dicts: {name, status, language, category, components}
    """
    if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
        return []

    # The Business Account ID is needed; we derive it from the phone number ID
    # by querying the phone number endpoint first
    url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{WHATSAPP_PHONE_NUMBER_ID}/message_templates"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    templates = data.get("data", [])
                    logger.info(f"Fetched {len(templates)} WhatsApp templates")
                    return templates
                else:
                    body = await resp.text()
                    logger.warning(f"Template fetch failed {resp.status}: {body}")
                    return []
    except Exception as e:
        logger.error(f"Failed to fetch templates: {e}")
        return []


async def send_followup_message(
    caller_phone: str,
    caller_name: str,
    handoff_requested: bool,
):
    """Send post-call follow-up message with thank-you and contact info."""
    if not caller_phone:
        return

    name = caller_name or "there"

    if handoff_requested:
        message = (
            f"Hi {name}! Thank you for calling Institute of Financial Studies.\n\n"
            f"We noticed you would like to speak with our team directly. "
            f"A team member will reach out to you shortly.\n\n"
            f"In the meantime, feel free to reach us at:\n"
            f"Phone: +91 78913 93505\n"
            f"Mon-Sat: 10 AM - 6 PM\n\n"
            f"Thank you for your interest in IFS!"
        )
    else:
        message = (
            f"Hi {name}! Thank you for calling Institute of Financial Studies.\n\n"
            f"If you have any more questions, feel free to call us again or reach out at:\n"
            f"Phone: +91 78913 93505\n"
            f"Mon-Sat: 10 AM - 6 PM\n\n"
            f"We look forward to hearing from you!"
        )

    await send_whatsapp_text(caller_phone, message)
