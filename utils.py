"""Shared Utilities

Common functions used by both the voice bot (bot.py) and text chatbot (chatbot.py):
handoff detection, topic extraction, and ID generation.
"""

import uuid

# Phrases that indicate the caller explicitly asked for a human agent.
# Only trigger on clear handoff commitments, not general "connect with admissions" responses.
HANDOFF_PHRASES = [
    "someone will call you back",
    "have someone call you back",
    "let our team know",
    "our team will contact you",
    "transfer you to",
    "connect you with a person",
]

# Keywords for topic extraction from transcript
TOPIC_KEYWORDS = {
    "courses": ["course", "program", "certification", "class", "training", "study"],
    "admissions": ["admission", "enroll", "registration", "apply", "seat", "batch"],
    "fees": ["fee", "cost", "price", "payment", "installment", "scholarship"],
    "placements": ["placement", "job", "career", "salary", "hiring"],
    "corporate_training": ["corporate", "company", "organization", "team training"],
    "schedule": ["timing", "schedule", "when", "date", "start", "duration"],
    "location": ["address", "location", "where", "office", "branch", "jaipur"],
    "contact": ["phone", "email", "contact", "call back", "reach"],
}


def generate_id() -> str:
    """Generate a unique ID for calls or conversations."""
    return str(uuid.uuid4())


def detect_handoff(transcript: list) -> tuple[bool, str]:
    """Check if any assistant message indicates handoff was triggered.

    Args:
        transcript: List of dicts with 'role' and 'content' keys.

    Returns:
        Tuple of (handoff_requested, reason_string).
    """
    for msg in transcript:
        if msg["role"] == "assistant":
            content_lower = msg["content"].lower()
            for phrase in HANDOFF_PHRASES:
                if phrase in content_lower:
                    return True, f"Assistant offered handoff: '{phrase}'"
    return False, ""


def extract_topics(transcript: list) -> list[str]:
    """Extract key topics from transcript using keyword matching.

    Args:
        transcript: List of dicts with 'role' and 'content' keys.

    Returns:
        List of topic strings found in the conversation.
    """
    all_text = " ".join(
        msg["content"].lower() for msg in transcript if msg.get("content")
    )
    found = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw in all_text for kw in keywords):
            found.append(topic)
    return found
