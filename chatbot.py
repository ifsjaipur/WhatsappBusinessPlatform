"""WhatsApp Text Chatbot — GPT-4o

Handles incoming WhatsApp text messages with OpenAI GPT-4o.
Multi-turn conversations, bilingual (Hindi/English), same knowledge base
as the voice agent, with handoff detection and n8n hooks.
"""

import json
import os

from loguru import logger
from openai import AsyncOpenAI

from chat_db import (
    add_message,
    check_duplicate_message,
    get_or_create_conversation,
    get_recent_messages,
    update_conversation,
)
from hooks import send_chat_summary
from utils import detect_handoff, extract_topics
from whatsapp_messaging import mark_message_as_read, send_whatsapp_text

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

CHAT_SYSTEM_PROMPT = """You are an AI assistant for Institute of Financial Studies (IFS), Jaipur.

IMPORTANT RULES:
- You are responding to WhatsApp text messages. Keep replies clear and helpful (3-5 sentences).
- Respond in the SAME LANGUAGE the user writes in (Hindi or English).
- Only answer from the knowledge provided below. Never make up information.
- If you don't know something, say: "I'd be happy to connect you with our admissions team for more details. You can also reach us at +91 78913 93505."
- You may use bullet points or numbered lists for clarity.
- Be warm, professional, and helpful.
- If the user wants to speak to a human, say: "I'll let our team know. Someone will call you back shortly."

YOUR KNOWLEDGE:
{knowledge}
"""

FALLBACK_MESSAGE = (
    "I'm sorry, I'm having trouble processing your message right now. "
    "Please try again or call us directly at +91 78913 93505."
)


async def handle_text_message(
    sender_phone: str,
    sender_name: str,
    message_text: str,
    wa_message_id: str,
    knowledge_context: str,
):
    """Process an incoming WhatsApp text message and reply with GPT-4o.

    Args:
        sender_phone: Sender's phone number (e.g. '919876543210')
        sender_name: Sender's WhatsApp profile name
        message_text: The text message content
        wa_message_id: WhatsApp message ID (for dedup + read receipts)
        knowledge_context: Knowledge base content for the system prompt
    """
    logger.info(f"Chat from {sender_phone} ({sender_name}): {message_text[:100]}")

    # 1. Send read receipt (blue ticks)
    if wa_message_id:
        await mark_message_as_read(wa_message_id)

    # 2. Deduplicate
    if await check_duplicate_message(wa_message_id):
        logger.info(f"Duplicate message {wa_message_id}, skipping")
        return

    # 3. Get or create conversation
    conversation = await get_or_create_conversation(sender_phone, sender_name)
    conv_id = conversation["id"]
    logger.info(f"Conversation {conv_id} for {sender_phone}")

    # 4. Store user message
    await add_message(conv_id, "user", message_text, wa_message_id)

    # 5. Load recent messages for context
    recent_messages = await get_recent_messages(conv_id, limit=10)

    # 6. Call GPT-4o
    reply_text = await _call_gpt4o(knowledge_context, recent_messages)

    # 7. Store assistant reply
    await add_message(conv_id, "assistant", reply_text)

    # 8. Send reply via WhatsApp
    sent = await send_whatsapp_text(sender_phone, reply_text)
    if not sent:
        logger.error(f"Failed to send reply to {sender_phone}")

    # 9. Run handoff detection + topic extraction
    all_messages = await get_recent_messages(conv_id, limit=50)
    transcript = [{"role": m["role"], "content": m["content"]} for m in all_messages]

    handoff_requested, handoff_reason = detect_handoff(transcript)
    topics = extract_topics(transcript)

    update_fields = {
        "topics": json.dumps(topics, ensure_ascii=False),
        "name": sender_name or conversation.get("name", ""),
    }

    if handoff_requested and not conversation.get("handoff_requested"):
        update_fields["handoff_requested"] = 1
        update_fields["handoff_reason"] = handoff_reason
        update_fields["status"] = "handoff_pending"
        logger.info(f"Chat {conv_id}: HANDOFF DETECTED — {handoff_reason}")

        # Send n8n hook for handoff
        try:
            await send_chat_summary({
                "conversation_id": conv_id,
                "phone": sender_phone,
                "name": sender_name,
                "handoff_requested": True,
                "handoff_reason": handoff_reason,
                "topics": topics,
                "message_count": len(all_messages),
                "last_message": message_text,
            })
        except Exception as e:
            logger.error(f"Chat {conv_id}: Failed to send chat hook: {e}")

    await update_conversation(conv_id, **update_fields)


async def _call_gpt4o(knowledge_context: str, messages: list[dict]) -> str:
    """Call GPT-4o with conversation history and return the reply."""
    if not OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY not set")
        return FALLBACK_MESSAGE

    system_prompt = CHAT_SYSTEM_PROMPT.format(
        knowledge=knowledge_context or "No knowledge documents loaded yet."
    )

    openai_messages = [{"role": "system", "content": system_prompt}]
    for msg in messages:
        openai_messages.append({"role": msg["role"], "content": msg["content"]})

    try:
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=openai_messages,
            max_tokens=500,
            temperature=0.7,
        )
        reply = response.choices[0].message.content.strip()
        logger.info(f"GPT-4o reply: {reply[:100]}...")
        return reply
    except Exception as e:
        logger.error(f"GPT-4o call failed: {e}")
        return FALLBACK_MESSAGE
