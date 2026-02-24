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
