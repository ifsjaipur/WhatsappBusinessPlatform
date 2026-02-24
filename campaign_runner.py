"""Campaign Runner — Rate-Limited Bulk Template Sender

Processes campaign sending in the background with configurable
rate limiting. Tracks per-recipient delivery status and updates
campaign stats in real time.
"""

import asyncio
import time

from loguru import logger

from campaign_db import (
    get_campaign,
    get_pending_recipients,
    refresh_campaign_stats,
    update_campaign,
    update_recipient_status,
)
from whatsapp_messaging import send_whatsapp_template

# Global tracking of running campaigns (campaign_id → should_pause flag)
_running_campaigns: dict[str, bool] = {}


def is_campaign_running(campaign_id: str) -> bool:
    return campaign_id in _running_campaigns


def request_pause(campaign_id: str):
    """Signal a running campaign to pause after current batch."""
    if campaign_id in _running_campaigns:
        _running_campaigns[campaign_id] = True


async def run_campaign(campaign_id: str) -> None:
    """Main entry point — sends templates to all pending recipients.

    Respects rate limit and checks for pause signals between batches.
    Should be called via background_tasks.add_task().
    """
    campaign = await get_campaign(campaign_id)
    if not campaign:
        logger.error(f"Campaign {campaign_id} not found")
        return

    if campaign["status"] not in ("draft", "paused"):
        logger.warning(f"Campaign {campaign_id} status is {campaign['status']}, cannot start")
        return

    rate_limit = campaign.get("rate_limit_per_min", 60)
    template_name = campaign["template_name"]
    language = campaign.get("language", "en")
    template_params = campaign.get("template_params") or []

    # Mark as running
    _running_campaigns[campaign_id] = False
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    await update_campaign(campaign_id, status="running", started_at=now)
    logger.info(f"Campaign {campaign_id} started (rate: {rate_limit}/min)")

    min_interval = 60.0 / max(rate_limit, 1)
    batch_size = 50
    total_sent = 0
    total_failed = 0

    try:
        while True:
            # Check pause signal
            if _running_campaigns.get(campaign_id, False):
                await update_campaign(campaign_id, status="paused")
                await refresh_campaign_stats(campaign_id)
                logger.info(f"Campaign {campaign_id} paused after {total_sent} sent")
                break

            # Get next batch
            recipients = await get_pending_recipients(campaign_id, limit=batch_size)
            if not recipients:
                # All done
                now_str = datetime.now(timezone.utc).isoformat()
                await update_campaign(campaign_id, status="completed", completed_at=now_str)
                await refresh_campaign_stats(campaign_id)
                logger.info(f"Campaign {campaign_id} completed: {total_sent} sent, {total_failed} failed")
                break

            for recipient in recipients:
                # Check pause signal between each send
                if _running_campaigns.get(campaign_id, False):
                    break

                rid = recipient["id"]
                phone = recipient["phone"]
                name = recipient.get("name", "")

                # Build template components with recipient-specific params
                components = _build_components(template_params, name)

                try:
                    send_start = time.monotonic()
                    success = await send_whatsapp_template(
                        to_phone=phone,
                        template_name=template_name,
                        language=language,
                        components=components if components else None,
                    )

                    if success:
                        await update_recipient_status(rid, "sent")
                        total_sent += 1
                    else:
                        await update_recipient_status(rid, "failed", error_message="API returned error")
                        total_failed += 1

                    # Rate limiting
                    elapsed = time.monotonic() - send_start
                    sleep_time = min_interval - elapsed
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

                except Exception as e:
                    logger.error(f"Campaign {campaign_id}: send to {phone} failed: {e}")
                    await update_recipient_status(rid, "failed", error_message=str(e)[:200])
                    total_failed += 1

            # Refresh stats after each batch
            await refresh_campaign_stats(campaign_id)

    except Exception as e:
        logger.error(f"Campaign {campaign_id} runner error: {e}")
        await update_campaign(campaign_id, status="failed")
        await refresh_campaign_stats(campaign_id)
    finally:
        _running_campaigns.pop(campaign_id, None)


def _build_components(template_params: list, recipient_name: str) -> list:
    """Build WhatsApp template components from parameter definitions.

    template_params is a list of parameter values (strings).
    {{name}} in values is replaced with the recipient's name.
    """
    if not template_params:
        return []

    body_params = []
    for param in template_params:
        value = str(param).replace("{{name}}", recipient_name or "there")
        body_params.append({"type": "text", "text": value})

    if not body_params:
        return []

    return [{"type": "body", "parameters": body_params}]
