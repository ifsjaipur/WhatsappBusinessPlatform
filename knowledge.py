"""Knowledge Base Loader

Reads all markdown files from the knowledge/ directory and combines them
into a single context string for the AI agent's system prompt.

Files are re-read on each call so you can update docs without restarting the server.
"""

import os
from pathlib import Path

from loguru import logger

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


def load_knowledge() -> str:
    """Load all .md files from knowledge/ directory.

    Returns:
        Combined content of all knowledge documents.
    """
    if not KNOWLEDGE_DIR.exists():
        logger.warning(f"Knowledge directory not found: {KNOWLEDGE_DIR}")
        return ""

    documents = []
    for md_file in sorted(KNOWLEDGE_DIR.glob("*.md")):
        try:
            content = md_file.read_text(encoding="utf-8").strip()
            if content:
                documents.append(f"--- {md_file.stem.upper()} ---\n{content}")
                logger.debug(f"Loaded knowledge: {md_file.name} ({len(content)} chars)")
        except Exception as e:
            logger.error(f"Failed to read {md_file}: {e}")

    if not documents:
        logger.warning("No knowledge documents found")
        return ""

    combined = "\n\n".join(documents)
    logger.info(f"Loaded {len(documents)} knowledge docs ({len(combined)} chars total)")
    return combined
