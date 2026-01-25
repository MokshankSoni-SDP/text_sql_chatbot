import os
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
import logging
from pathlib import Path
from .db_connection import get_db_instance

# Load environment variables from project root
current_dir = Path(__file__).resolve().parent.parent
env_path = current_dir / '.env'
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChatHistoryManager:
    """
    Manages chat history storage and retrieval.
    """
    
    def __init__(self):
        """Initialize chat history manager."""
        self.db = get_db_instance()
        self.history_limit = int(os.getenv('CHAT_HISTORY_LIMIT', '5'))
    
    def insert_message(self, session_id: str, role: str, content: str, llm_client=None) -> bool:
        """
        Insert a chat message into the database with smart summarization.
        
        Args:
            session_id: Unique session identifier
            role: Message role ('user' or 'assistant')
            content: Message content
            llm_client: Optional LLM client for smart summarization
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Smart summarization for long assistant responses
        content_to_store = content
        
        if role == 'assistant' and len(content) > 300 and llm_client is not None:
            try:
                logger.info(f"📝 Summarizing long assistant response ({len(content)} chars)...")
                content_to_store = llm_client.summarize_text(content)
                logger.info(f"✅ Summarized to {len(content_to_store)} chars")
            except Exception as e:
                logger.warning(f"⚠️ Summarization failed, storing original content: {e}")
                content_to_store = content  # Fallback to original
        
        query = """
            INSERT INTO chat_history (session_id, role, content, timestamp)
            VALUES (%s, %s, %s, %s);
        """
        
        try:
            self.db.execute_query(
                query,
                (session_id, role, content_to_store, datetime.now()),
                fetch=False
            )
            logger.info(f"✅ Inserted {role} message for session {session_id[:8]}...")
            return True
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check for common table-related errors
            if 'does not exist' in error_msg or 'relation' in error_msg:
                logger.error(f"❌ chat_history table does not exist! Run fix_chat_history.sql to create it.")
                logger.error(f"   Error: {e}")
            elif 'permission' in error_msg:
                logger.error(f"❌ Permission denied to insert into chat_history table")
                logger.error(f"   Error: {e}")
            else:
                logger.error(f"❌ Error inserting message: {e}")
            
            return False
    
    def get_recent_messages(self, session_id: str, limit: int = None) -> List[Dict[str, str]]:
        """
        Retrieve recent chat messages for a session.
        
        Args:
            session_id: Unique session identifier
            limit: Maximum number of messages to retrieve (uses CHAT_HISTORY_LIMIT if None)
            
        Returns:
            List[Dict]: List of message dictionaries with 'role' and 'content'
        """
        if limit is None:
            limit = self.history_limit
        
        query = """
            SELECT role, content, timestamp
            FROM chat_history
            WHERE session_id = %s
            ORDER BY timestamp DESC
            LIMIT %s;
        """
        
        try:
            results = self.db.execute_query(query, (session_id, limit))
            
            # Reverse to get chronological order
            messages = [
                {
                    'role': row[0],
                    'content': row[1],
                    'timestamp': row[2]
                }
                for row in reversed(results)
            ]
            
            logger.debug(f"Retrieved {len(messages)} messages for session {session_id}")
            return messages
        except Exception as e:
            logger.error(f"Error retrieving messages: {e}")
            return []
    
    def clear_session_history(self, session_id: str) -> bool:
        """
        Clear all chat history for a specific session.
        
        Args:
            session_id: Unique session identifier
            
        Returns:
            bool: True if successful, False otherwise
        """
        query = "DELETE FROM chat_history WHERE session_id = %s;"
        
        try:
            self.db.execute_query(query, (session_id,), fetch=False)
            logger.info(f"Cleared history for session {session_id}")
            return True
        except Exception as e:
            logger.error(f"Error clearing session history: {e}")
            return False
    
    
    def format_history_for_llm(self, session_id: str, limit: int = None) -> str:
        """
        Format chat history as a string for LLM context.
        Note: Messages are already intelligently summarized at insert time via LLM.
        
        Args:
            session_id: Unique session identifier
            limit: Maximum number of messages to include
            
        Returns:
            str: Formatted chat history
        """
        if limit is None:
            limit = self.history_limit
        
        messages = self.get_recent_messages(session_id, limit)
        
        if not messages:
            return "No previous conversation history."
        
        formatted_parts = ["PREVIOUS CONVERSATION (Recent context):"]
        
        for msg in messages:
            role = msg['role'].upper()
            content = msg['content']
            formatted_parts.append(f"{role}: {content}")
        
        return "\n".join(formatted_parts)


def get_chat_history_manager() -> ChatHistoryManager:
    """
    Get chat history manager instance.
    
    Returns:
        ChatHistoryManager: Chat history manager instance
    """
    return ChatHistoryManager()
