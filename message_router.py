"""Unified WhatsApp Webhook Router

Central routing logic that replaces the n8n Switch node. Parses incoming
WhatsApp webhook events and routes them to the appropriate handler:

- text messages → AI chatbot or manual inbox (based on ai_enabled)
- call events → forwarded to Pipecat voice agent (handled by server.py)
- delivery statuses → update message status in DB + campaign tracking
- media → store and acknowledge
- interactive → store reply (button/list)
- order → full order processing with Razorpay payment

All message types auto-create a contact in the mini CRM.
"""

from loguru import logger

from campaign_db import update_recipient_by_wamid
from chat_db import add_message, get_or_create_conversation, update_message_status
from chatbot import handle_text_message
from contacts_db import get_or_create_contact
from orders import process_incoming_order
from whatsapp_messaging import mark_message_as_read, send_whatsapp_text


async def route_webhook(body: dict, knowledge_context: str) -> dict:
    """Parse a WhatsApp webhook body and route to the correct handler.

    Args:
        body: Raw WhatsApp webhook JSON payload.
        knowledge_context: Current knowledge base text for AI chatbot.

    Returns:
        Dict with routing result: {"action": ..., "details": ...}
    """
    entries = body.get("entry", [])
    if not entries:
        return {"action": "skipped", "reason": "no entries"}

    for entry in entries:
        for change in entry.get("changes", []):
            value = change.get("value", {})
            field = change.get("field", "messages")

            # --- Call events are handled separately by server.py ---
            if field == "calls":
                return {"action": "call_event", "reason": "handled by voice endpoint"}

            # --- Delivery / read status updates ---
            statuses = value.get("statuses", [])
            if statuses and not value.get("messages"):
                await _handle_statuses(statuses)
                return {"action": "status_update", "count": len(statuses)}

            # --- Messages ---
            messages = value.get("messages", [])
            if not messages:
                continue

            # Extract sender info
            contacts = value.get("contacts", [])
            sender_phone = ""
            sender_name = ""
            if contacts:
                sender_phone = contacts[0].get("wa_id", "")
                profile = contacts[0].get("profile", {})
                sender_name = profile.get("name", "")

            msg = messages[0]
            sender_phone = sender_phone or msg.get("from", "")
            wa_message_id = msg.get("id", "")
            msg_type = msg.get("type", "")

            if not sender_phone:
                logger.warning("No sender phone in webhook, skipping")
                continue

            # Get or create contact (mini CRM)
            contact = await get_or_create_contact(sender_phone, sender_name)
            contact_id = contact["id"]
            ai_enabled = contact.get("ai_enabled", 1)

            logger.info(
                f"Router: {msg_type} from {sender_phone} ({sender_name}) "
                f"[contact={contact_id}, ai={'on' if ai_enabled else 'off'}]"
            )

            # Route by message type
            if msg_type == "text":
                return await _handle_text(
                    sender_phone, sender_name, msg, wa_message_id,
                    contact_id, ai_enabled, knowledge_context,
                )

            if msg_type in ("image", "audio", "video", "document", "sticker"):
                return await _handle_media(
                    sender_phone, sender_name, msg, msg_type,
                    wa_message_id, contact_id,
                )

            if msg_type == "interactive":
                return await _handle_interactive(
                    sender_phone, sender_name, msg,
                    wa_message_id, contact_id,
                )

            if msg_type == "order":
                return await _handle_order(
                    sender_phone, sender_name, msg,
                    wa_message_id, contact_id,
                )

            if msg_type == "reaction":
                logger.debug(f"Reaction from {sender_phone}, ignoring")
                return {"action": "skipped", "reason": "reaction"}

            # Unknown message type — log and skip
            logger.info(f"Unhandled message type: {msg_type}")
            return {"action": "skipped", "reason": f"unhandled type: {msg_type}"}

    return {"action": "skipped", "reason": "no actionable content"}


async def _handle_text(
    sender_phone: str,
    sender_name: str,
    msg: dict,
    wa_message_id: str,
    contact_id: str,
    ai_enabled: int,
    knowledge_context: str,
) -> dict:
    """Handle incoming text message — route to AI or just store for manual inbox."""
    message_text = msg.get("text", {}).get("body", "")
    if not message_text:
        return {"action": "skipped", "reason": "empty text"}

    if ai_enabled:
        # AI chatbot handles the full flow (store, reply, handoff detection)
        await handle_text_message(
            sender_phone=sender_phone,
            sender_name=sender_name,
            message_text=message_text,
            wa_message_id=wa_message_id,
            knowledge_context=knowledge_context,
            contact_id=contact_id,
        )
        return {"action": "ai_reply", "phone": sender_phone}
    else:
        # AI disabled — store message for manual reply via inbox
        if wa_message_id:
            await mark_message_as_read(wa_message_id)

        conversation = await get_or_create_conversation(sender_phone, sender_name, contact_id)
        conv_id = conversation["id"]
        await add_message(
            conv_id, "user", message_text, wa_message_id,
            direction="inbound", source="user",
        )
        logger.info(f"Stored message for manual reply: {sender_phone} → conv {conv_id}")
        return {"action": "stored_for_manual", "phone": sender_phone, "conversation_id": conv_id}


async def _handle_media(
    sender_phone: str,
    sender_name: str,
    msg: dict,
    msg_type: str,
    wa_message_id: str,
    contact_id: str,
) -> dict:
    """Handle media messages — store a reference and acknowledge."""
    if wa_message_id:
        await mark_message_as_read(wa_message_id)

    # Extract media info
    media_obj = msg.get(msg_type, {})
    media_id = media_obj.get("id", "")
    mime_type = media_obj.get("mime_type", "")
    caption = media_obj.get("caption", "")

    content = f"[{msg_type.upper()}] {caption}" if caption else f"[{msg_type.upper()} received]"
    if media_id:
        content += f" (media_id: {media_id}, type: {mime_type})"

    # Store in conversation
    conversation = await get_or_create_conversation(sender_phone, sender_name, contact_id)
    conv_id = conversation["id"]
    await add_message(
        conv_id, "user", content, wa_message_id,
        direction="inbound", source="user",
    )

    # Auto-reply acknowledging the media
    reply = "We received your file. Our team will review it. For immediate help, call us at +91 78913 93505."
    await send_whatsapp_text(sender_phone, reply)
    await add_message(conv_id, "assistant", reply, direction="outbound", source="ai")

    logger.info(f"Media ({msg_type}) from {sender_phone} stored in conv {conv_id}")
    return {"action": "media_stored", "type": msg_type, "phone": sender_phone}


async def _handle_interactive(
    sender_phone: str,
    sender_name: str,
    msg: dict,
    wa_message_id: str,
    contact_id: str,
) -> dict:
    """Handle interactive replies (list_reply, button_reply). Store for now."""
    if wa_message_id:
        await mark_message_as_read(wa_message_id)

    interactive = msg.get("interactive", {})
    reply_type = interactive.get("type", "")

    # Extract reply content
    reply_data = interactive.get(reply_type, {})
    reply_id = reply_data.get("id", "")
    reply_title = reply_data.get("title", "")

    content = f"[Interactive: {reply_type}] {reply_title} (id: {reply_id})"

    conversation = await get_or_create_conversation(sender_phone, sender_name, contact_id)
    conv_id = conversation["id"]
    await add_message(
        conv_id, "user", content, wa_message_id,
        direction="inbound", source="user",
    )

    logger.info(f"Interactive ({reply_type}) from {sender_phone}: {reply_title}")
    return {"action": "interactive_stored", "type": reply_type, "phone": sender_phone}


async def _handle_order(
    sender_phone: str,
    sender_name: str,
    msg: dict,
    wa_message_id: str,
    contact_id: str,
) -> dict:
    """Handle WhatsApp catalog order — process with Razorpay payment flow."""
    if wa_message_id:
        await mark_message_as_read(wa_message_id)

    order_data = msg.get("order", {})
    product_items = order_data.get("product_items", [])

    items_summary = ", ".join(
        f"{item.get('product_retailer_id', '?')} x{item.get('quantity', 1)}"
        for item in product_items
    )

    # Store order message in conversation
    conversation = await get_or_create_conversation(sender_phone, sender_name, contact_id)
    conv_id = conversation["id"]
    content = f"[ORDER] Items: {items_summary}"
    await add_message(
        conv_id, "user", content, wa_message_id,
        direction="inbound", source="user",
    )

    # Process order (creates DB record, Razorpay link, sends payment msg)
    try:
        result = await process_incoming_order(
            phone=sender_phone,
            name=sender_name,
            contact_id=contact_id,
            conversation_id=conv_id,
            order_data=order_data,
        )

        # Store payment message in conversation
        if result.get("payment_link"):
            await add_message(
                conv_id, "assistant",
                f"Payment link sent: {result['payment_link']} (₹{result['total_amount']:,.0f})",
                direction="outbound", source="system",
            )

        logger.info(f"Order processed: {result.get('order_id')} for {sender_phone}")
        return {"action": "order_processed", "phone": sender_phone, **result}

    except Exception as e:
        logger.error(f"Order processing failed for {sender_phone}: {e}")
        # Fallback acknowledgment
        reply = "We received your order but encountered a processing issue. Our team will contact you shortly."
        await send_whatsapp_text(sender_phone, reply)
        await add_message(conv_id, "assistant", reply, direction="outbound", source="system")
        return {"action": "order_error", "phone": sender_phone, "error": str(e)}


async def _handle_statuses(statuses: list) -> None:
    """Track delivery statuses (sent, delivered, read, failed).

    Updates both conversation messages and campaign recipients.
    """
    for status_obj in statuses:
        wa_message_id = status_obj.get("id", "")
        status = status_obj.get("status", "")  # sent, delivered, read, failed
        if wa_message_id and status:
            # Update conversation message status
            await update_message_status(wa_message_id, status)

            # Also update campaign recipient status (if this was a campaign message)
            try:
                await update_recipient_by_wamid(wa_message_id, status)
            except Exception:
                pass  # Not a campaign message, that's fine

            logger.debug(f"Status update: {wa_message_id} → {status}")
