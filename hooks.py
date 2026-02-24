"""Post-Call/Chat Hooks

Sends enriched call and chat data to n8n webhooks.
n8n can then trigger automations: email brief, Telegram notification,
Google Sheet log, lead creation, handoff alerts, etc.
"""

import os

import aiohttp
from loguru import logger

N8N_CALL_HOOK_URL = os.getenv("N8N_CALL_HOOK_URL", "")
N8N_CHAT_HOOK_URL = os.getenv("N8N_CHAT_HOOK_URL", "")


async def send_call_summary(call_data: dict):
    """Send enriched call data to n8n after a call ends.

    Args:
        call_data: Dict with full call info including:
            - call_id, caller_phone, caller_name
            - connected_at, disconnected_at, duration_seconds
            - transcript (list of {role, content} dicts)
            - handoff_requested (bool), handoff_reason (str)
            - topics (list of strings)
            - recording_path (str)
    """
    if not N8N_CALL_HOOK_URL:
        logger.debug("N8N_CALL_HOOK_URL not set, skipping call summary hook")
        return

    payload = {
        "event": "call_ended",
        "call_id": call_data.get("call_id", ""),
        "caller": {
            "phone": call_data.get("caller_phone", ""),
            "name": call_data.get("caller_name", ""),
        },
        "timing": {
            "connected_at": call_data.get("connected_at"),
            "disconnected_at": call_data.get("disconnected_at"),
            "duration_seconds": call_data.get("duration_seconds", 0),
        },
        "transcript": call_data.get("transcript", []),
        "handoff": {
            "requested": call_data.get("handoff_requested", False),
            "reason": call_data.get("handoff_reason", ""),
        },
        "topics": call_data.get("topics", []),
        "recording_path": call_data.get("recording_path", ""),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                N8N_CALL_HOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    logger.info(f"Call summary sent to n8n: call_id={call_data.get('call_id')}")
                else:
                    body = await resp.text()
                    logger.warning(f"n8n hook returned {resp.status}: {body}")
    except Exception as e:
        logger.error(f"Failed to send call summary to n8n: {e}")


async def send_chat_summary(chat_data: dict):
    """Send chat handoff notification to n8n.

    Args:
        chat_data: Dict with:
            - conversation_id, phone, name
            - handoff_requested (bool), handoff_reason (str)
            - topics (list), message_count (int), last_message (str)
    """
    if not N8N_CHAT_HOOK_URL:
        logger.debug("N8N_CHAT_HOOK_URL not set, skipping chat summary hook")
        return

    payload = {
        "event": "chat_handoff",
        "conversation_id": chat_data.get("conversation_id", ""),
        "contact": {
            "phone": chat_data.get("phone", ""),
            "name": chat_data.get("name", ""),
        },
        "handoff": {
            "requested": chat_data.get("handoff_requested", False),
            "reason": chat_data.get("handoff_reason", ""),
        },
        "topics": chat_data.get("topics", []),
        "message_count": chat_data.get("message_count", 0),
        "last_message": chat_data.get("last_message", ""),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                N8N_CHAT_HOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    logger.info(f"Chat summary sent to n8n: conv={chat_data.get('conversation_id')}")
                else:
                    body = await resp.text()
                    logger.warning(f"n8n chat hook returned {resp.status}: {body}")
    except Exception as e:
        logger.error(f"Failed to send chat summary to n8n: {e}")
