"""Post-Call Hooks

Sends call summary to n8n webhook after each call ends.
n8n can then trigger automations: email brief, Telegram notification,
Google Sheet log, lead creation, etc.
"""

import os

import aiohttp
from loguru import logger

N8N_CALL_HOOK_URL = os.getenv("N8N_CALL_HOOK_URL", "")


async def send_call_summary(call_metadata: dict):
    """Send call summary to n8n after a call ends.

    Args:
        call_metadata: Dict with call info (connected_at, disconnected_at, etc.)
    """
    if not N8N_CALL_HOOK_URL:
        logger.debug("N8N_CALL_HOOK_URL not set, skipping call summary hook")
        return

    payload = {
        "event": "call_ended",
        "connected_at": call_metadata.get("connected_at"),
        "disconnected_at": call_metadata.get("disconnected_at"),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(N8N_CALL_HOOK_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    logger.info(f"Call summary sent to n8n: {resp.status}")
                else:
                    body = await resp.text()
                    logger.warning(f"n8n hook returned {resp.status}: {body}")
    except Exception as e:
        logger.error(f"Failed to send call summary to n8n: {e}")
