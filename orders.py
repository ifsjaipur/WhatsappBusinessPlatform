"""Order Processing — WhatsApp Catalog Orders + Razorpay Payments

Handles the full order lifecycle:
1. Incoming WhatsApp order message → parse items, calculate total
2. Create Razorpay payment order → get payment link
3. Send payment link to user via WhatsApp
4. Razorpay webhook → mark payment completed → send confirmation

Razorpay integration is optional — if RAZORPAY_KEY_ID is not set,
orders are stored but no payment link is generated.
"""

import hashlib
import hmac
import json
import os

import aiohttp
from loguru import logger

from contacts_db import update_contact
from orders_db import (
    create_order,
    get_order,
    get_order_by_razorpay_id,
    mark_payment_completed,
    mark_payment_failed,
    set_razorpay_details,
)
from whatsapp_messaging import send_whatsapp_text

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")
RAZORPAY_API_URL = "https://api.razorpay.com/v1"


async def process_incoming_order(
    phone: str,
    name: str,
    contact_id: str,
    conversation_id: str,
    order_data: dict,
) -> dict:
    """Process an incoming WhatsApp catalog order.

    Args:
        phone: Sender phone number
        name: Sender name
        contact_id: Contact ID from CRM
        conversation_id: Active conversation ID
        order_data: The 'order' object from WhatsApp message

    Returns:
        Dict with order_id, total_amount, payment_link (if Razorpay configured)
    """
    catalog_id = order_data.get("catalog_id", "")
    product_items = order_data.get("product_items", [])

    # Parse items and calculate total
    items = []
    total_amount = 0
    for item in product_items:
        retailer_id = item.get("product_retailer_id", "")
        quantity = int(item.get("quantity", 1))
        # item_price is in paise (1/100 of currency unit) from WhatsApp
        item_price = float(item.get("item_price", 0))
        currency = item.get("currency", "INR")

        items.append({
            "retailer_id": retailer_id,
            "quantity": quantity,
            "item_price": item_price,
            "currency": currency,
        })
        total_amount += item_price * quantity

    # Convert from paise to rupees for display (WhatsApp sends in paise)
    total_rupees = total_amount / 100 if total_amount > 1000 else total_amount

    # Create order in database
    order = await create_order(
        contact_id=contact_id,
        phone=phone,
        name=name,
        conversation_id=conversation_id,
        catalog_id=catalog_id,
        items=items,
        total_amount=total_rupees,
        currency="INR",
    )
    order_id = order["id"]
    logger.info(f"Order {order_id}: {len(items)} items, total ₹{total_rupees}")

    result = {
        "order_id": order_id,
        "total_amount": total_rupees,
        "item_count": len(items),
        "payment_link": "",
    }

    # Create Razorpay order if configured
    if RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET:
        try:
            rz_result = await _create_razorpay_order(order_id, total_rupees, phone, name)
            if rz_result:
                result["payment_link"] = rz_result.get("short_url", "")
                result["razorpay_order_id"] = rz_result.get("id", "")

                # Send payment link to user
                payment_msg = (
                    f"Thank you for your order! Your total is ₹{total_rupees:,.0f}.\n\n"
                    f"Complete your payment here:\n{result['payment_link']}\n\n"
                    f"If you face any issues, call us at +91 78913 93505."
                )
                await send_whatsapp_text(phone, payment_msg)
            else:
                # Razorpay failed — send acknowledgment without payment link
                await send_whatsapp_text(
                    phone,
                    f"Thank you for your order (₹{total_rupees:,.0f})! "
                    f"Our team will share the payment link shortly. "
                    f"Call us at +91 78913 93505 for assistance."
                )
        except Exception as e:
            logger.error(f"Order {order_id}: Razorpay error: {e}")
            await send_whatsapp_text(
                phone,
                f"We received your order (₹{total_rupees:,.0f}). "
                f"Our team will process it and share payment details shortly."
            )
    else:
        # No Razorpay — just acknowledge
        await send_whatsapp_text(
            phone,
            f"Thank you for your order (₹{total_rupees:,.0f})! "
            f"Our team will contact you with payment details. "
            f"You can also reach us at +91 78913 93505."
        )

    return result


async def handle_razorpay_webhook(payload: dict, signature: str) -> dict:
    """Handle Razorpay payment webhook.

    Args:
        payload: Razorpay webhook payload
        signature: X-Razorpay-Signature header value

    Returns:
        Dict with status and order details
    """
    # Verify signature if secret is configured
    if RAZORPAY_WEBHOOK_SECRET:
        if not _verify_razorpay_signature(payload, signature):
            logger.warning("Razorpay webhook signature verification failed")
            return {"status": "error", "reason": "invalid signature"}

    event = payload.get("event", "")
    payment_entity = payload.get("payload", {}).get("payment", {}).get("entity", {})

    if not payment_entity:
        logger.debug(f"Razorpay webhook event: {event} (no payment entity)")
        return {"status": "skipped", "event": event}

    razorpay_order_id = payment_entity.get("order_id", "")
    razorpay_payment_id = payment_entity.get("id", "")
    amount = payment_entity.get("amount", 0)
    status = payment_entity.get("status", "")

    logger.info(f"Razorpay webhook: {event} | order={razorpay_order_id} payment={razorpay_payment_id} status={status}")

    if not razorpay_order_id:
        return {"status": "skipped", "reason": "no order_id"}

    # Find our order
    order = await get_order_by_razorpay_id(razorpay_order_id)
    if not order:
        logger.warning(f"Razorpay webhook: order not found for {razorpay_order_id}")
        return {"status": "error", "reason": "order not found"}

    order_id = order["id"]

    if event == "payment.captured" or status == "captured":
        # Payment successful
        await mark_payment_completed(order_id, razorpay_payment_id)

        # Update contact stage to enrolled
        if order.get("contact_id"):
            try:
                await update_contact(order["contact_id"], stage="enrolled")
            except Exception as e:
                logger.error(f"Failed to update contact stage: {e}")

        # Send confirmation to user
        amount_rupees = amount / 100 if amount > 1000 else amount
        await send_whatsapp_text(
            order["phone"],
            f"Payment received! ₹{amount_rupees:,.0f} confirmed.\n\n"
            f"Order ID: {order_id[:8]}\n"
            f"Thank you for choosing Institute of Financial Studies!\n\n"
            f"Our team will reach out with next steps shortly."
        )

        logger.info(f"Order {order_id}: payment completed (₹{amount_rupees})")
        return {"status": "payment_completed", "order_id": order_id}

    elif event == "payment.failed" or status == "failed":
        error_desc = payment_entity.get("error_description", "Payment failed")
        await mark_payment_failed(order_id, error_desc)

        # Notify user
        await send_whatsapp_text(
            order["phone"],
            f"Your payment could not be processed. Please try again or contact us at +91 78913 93505."
        )

        logger.info(f"Order {order_id}: payment failed — {error_desc}")
        return {"status": "payment_failed", "order_id": order_id}

    else:
        logger.debug(f"Razorpay webhook event {event} ignored for order {order_id}")
        return {"status": "noted", "event": event, "order_id": order_id}


async def _create_razorpay_order(
    order_id: str,
    amount_rupees: float,
    phone: str,
    name: str,
) -> dict | None:
    """Create a Razorpay payment link order.

    Returns dict with id, short_url, etc. or None on failure.
    """
    amount_paise = int(amount_rupees * 100)

    # Use Payment Links API (simpler than standard Orders API)
    payload = {
        "amount": amount_paise,
        "currency": "INR",
        "description": f"IFS Order {order_id[:8]}",
        "customer": {
            "contact": f"+{phone}" if not phone.startswith("+") else phone,
            "name": name or "Customer",
        },
        "notify": {
            "sms": False,
            "email": False,
            "whatsapp": False,  # We send ourselves via WhatsApp
        },
        "reminder_enable": True,
        "notes": {
            "order_id": order_id,
            "source": "whatsapp_catalog",
        },
        "callback_url": "",
        "callback_method": "",
    }

    auth = aiohttp.BasicAuth(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{RAZORPAY_API_URL}/payment_links",
                json=payload,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    rz_order_id = data.get("order_id", data.get("id", ""))
                    short_url = data.get("short_url", "")

                    # Store Razorpay details
                    await set_razorpay_details(order_id, rz_order_id, short_url)
                    logger.info(f"Razorpay link created: {short_url} (order={rz_order_id})")
                    return data
                else:
                    body = await resp.text()
                    logger.error(f"Razorpay API error {resp.status}: {body}")
                    return None
    except Exception as e:
        logger.error(f"Razorpay API call failed: {e}")
        return None


def _verify_razorpay_signature(payload: dict, signature: str) -> bool:
    """Verify Razorpay webhook signature using HMAC-SHA256."""
    if not RAZORPAY_WEBHOOK_SECRET or not signature:
        return True  # Skip verification if not configured

    try:
        payload_json = json.dumps(payload, separators=(",", ":"))
        expected = hmac.new(
            RAZORPAY_WEBHOOK_SECRET.encode(),
            payload_json.encode(),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)
    except Exception as e:
        logger.error(f"Razorpay signature verification error: {e}")
        return False
