import os
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
import logging
from .db_connection import get_db_instance

# Load environment variables
load_dotenv()

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
    
    def insert_message(self, session_id: str, role: str, content: str) -> bool:
        """
        Insert a chat message into the database.
        
        Args:
            session_id: Unique session identifier
            role: Message role ('user' or 'assistant')
            content: Message content
            
        Returns:
            bool: True if successful, False otherwise
        """
        query = """
            INSERT INTO chat_history (session_id, role, content, timestamp)
            VALUES (%s, %s, %s, %s);
        """
        
        try:
            self.db.execute_query(
                query,
                (session_id, role, content, datetime.now()),
                fetch=False
            )
            logger.debug(f"Inserted {role} message for session {session_id}")
            return True
        except Exception as e:
            logger.error(f"Error inserting message: {e}")
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
    
    def get_session_count(self, session_id: str) -> int:
        """
        Get the total number of messages for a session.
        
        Args:
            session_id: Unique session identifier
            
        Returns:
            int: Number of messages
        """
        query = "SELECT COUNT(*) FROM chat_history WHERE session_id = %s;"
        
        try:
            result = self.db.execute_query(query, (session_id,))
            count = result[0][0] if result else 0
            return count
        except Exception as e:
            logger.error(f"Error getting message count: {e}")
            return 0
    
    def format_history_for_llm(self, session_id: str, limit: int = None) -> str:
        """
        Format chat history as a string for LLM context.
        
        Args:
            session_id: Unique session identifier
            limit: Maximum number of messages to include
            
        Returns:
            str: Formatted chat history
        """
        messages = self.get_recent_messages(session_id, limit)
        
        if not messages:
            return "No previous conversation history."
        
        formatted_parts = ["PREVIOUS CONVERSATION:"]
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
