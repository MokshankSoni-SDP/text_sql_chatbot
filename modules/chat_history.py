import os
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
import logging
import logging
from pathlib import Path
import streamlit as st
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
    
    def __init__(self, schema_name: str = 'public'):
        """Initialize chat history manager with session support."""
        self.db = get_db_instance()
        self.schema_name = schema_name
        self.history_limit = int(os.getenv('CHAT_HISTORY_LIMIT', '10'))
        
        # Ensure tables exist
        self.ensure_tables()

    def ensure_tables(self):
        """
        Create chat_sessions and chat_history tables in the schema.
        Handles migration basics implicitly.
        """
        try:
            # 1. Create Sessions Table
            sessions_query = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.chat_sessions (
                    id VARCHAR(255) PRIMARY KEY,
                    project_schema VARCHAR(255),
                    name TEXT DEFAULT 'New Chat',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            self.db.execute_query(sessions_query, fetch=False)
            
            # 2. Create History Table
            history_query = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.chat_history (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(255) NOT NULL,
                    role VARCHAR(50) NOT NULL CHECK (role IN ('user', 'assistant')),
                    content TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """
            self.db.execute_query(history_query, fetch=False)
            
            # 3. Add FK Constraint if missing (Safe Migration)
            try:
                # Check if FK exists
                fk_check = f"""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_schema = '{self.schema_name}' 
                    AND table_name = 'chat_history' 
                    AND constraint_type = 'FOREIGN KEY';
                """
                constraints = self.db.execute_query(fk_check)
                if not constraints:
                     # Attempt to add FK. might fail if data mismatch.
                     alter_query = f"""
                        ALTER TABLE {self.schema_name}.chat_history 
                        ADD CONSTRAINT fk_session 
                        FOREIGN KEY (session_id) 
                        REFERENCES {self.schema_name}.chat_sessions(id) 
                        ON DELETE CASCADE;
                     """
                     self.db.execute_query(alter_query, fetch=False)
            except Exception as e:
                logger.warning(f"Could not add FK constraint (orphan data likely): {e}")

            # Create indexes
            index_query = f"""
                CREATE INDEX IF NOT EXISTS idx_chat_history_session 
                ON {self.schema_name}.chat_history(session_id);
            """
            self.db.execute_query(index_query, fetch=False)
            
            logger.debug(f"Ensured chat tables exist in schema: {self.schema_name}")
            
        except Exception as e:
            logger.error(f"Error initializing chat tables in {self.schema_name}: {e}")

    # ================= SESSION MANAGEMENT =================
    
    def create_session(self, session_id: str, name: str = "New Chat") -> bool:
        """Create a new chat session."""
        try:
            query = f"""
                INSERT INTO {self.schema_name}.chat_sessions (id, name, created_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """
            self.db.execute_query(query, (session_id, name, datetime.now()), fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error creating session {session_id}: {e}")
            return False

    def get_all_sessions(self) -> List[Dict]:
        """List all chat sessions for this schema."""
        try:
            query = f"""
                SELECT id, name, created_at 
                FROM {self.schema_name}.chat_sessions
                ORDER BY created_at DESC;
            """
            results = self.db.execute_query(query)
            return [
                {'id': row[0], 'name': row[1], 'created_at': row[2].strftime("%Y-%m-%d %H:%M") if row[2] else ""} 
                for row in results
            ]
        except Exception as e:
            logger.error(f"Error fetching sessions: {e}")
            return []

    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        try:
            del_hist = f"DELETE FROM {self.schema_name}.chat_history WHERE session_id = %s;"
            self.db.execute_query(del_hist, (session_id,), fetch=False)
            
            del_sess = f"DELETE FROM {self.schema_name}.chat_sessions WHERE id = %s;"
            self.db.execute_query(del_sess, (session_id,), fetch=False)
            logger.info(f"🗑️ Deleted session {session_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting session {session_id}: {e}")
            return False
            
    def update_session_name(self, session_id: str, new_name: str) -> bool:
        """Update the display name of a session."""
        try:
            query = f"UPDATE {self.schema_name}.chat_sessions SET name = %s WHERE id = %s;"
            self.db.execute_query(query, (new_name, session_id), fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error updating session name: {e}")
            return False
    
    def insert_message(self, session_id: str, role: str, content: str, llm_client=None) -> bool:
        """
        Insert a chat message into the database.
        Automatically creates the session if it doesn't exist (Self-Healing).
        """
        query = f"""
            INSERT INTO {self.schema_name}.chat_history (session_id, role, content, timestamp)
            VALUES (%s, %s, %s, %s);
        """
        
        try:
            # Ensure session exists first (Foreign Key integirty)
            self.create_session(session_id, "New Chat")
            
            # Insert Message
            self.db.execute_query(
                query,
                (session_id, role, content, datetime.now()),
                fetch=False
            )
            logger.info(f"✅ Inserted {role} message ({len(content)} chars) for session {session_id[:8]}...")
            
            # Auto-rename session logic could go here (e.g. use LLM to name chat after first user msg)
            if role == 'user':
                self._maybe_rename_session(session_id, content)
                
            return True
        except Exception as e:
            logger.error(f"Error inserting message: {e}")
            return False

    def _maybe_rename_session(self, session_id: str, first_message: str):
        """Renames session based on first user question (Simple logic)."""
        try:
            # Check if name is still default
            check = f"SELECT name FROM {self.schema_name}.chat_sessions WHERE id = %s;"
            res = self.db.execute_query(check, (session_id,))
            if res and res[0][0] == 'New Chat':
                # Generate simple title (first 30 chars)
                new_title = first_message[:30] + "..." if len(first_message) > 30 else first_message
                self.update_session_name(session_id, new_title)
        except:
            pass # Non-critical
    
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
        
        query = f"""
            SELECT role, content, timestamp
            FROM {self.schema_name}.chat_history
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
        query = f"DELETE FROM {self.schema_name}.chat_history WHERE session_id = %s;"
        
        try:
            self.db.execute_query(query, (session_id,), fetch=False)
            logger.info(f"Cleared history for session {session_id}")
            return True
        except Exception as e:
            logger.error(f"Error clearing session history: {e}")
            return False
    
    
    def format_history_for_llm(self, session_id: str, limit: int = None, llm_client=None) -> str:
        """
        Format chat history as a string for LLM context.
        IMPORTANT: Automatically summarizes long assistant messages (>500 chars) to keep context concise.
        This allows full messages to be stored but summarized versions to be passed to LLM.
        
        Args:
            session_id: Unique session identifier
            limit: Maximum number of messages to include
            llm_client: Optional LLM client for summarization (recommended)
            
        Returns:
            str: Formatted chat history with summarized long messages
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
            
            # Summarize long messages for LLM context
            # Aggressive summarization to save tokens (User Request)
            should_summarize = False
            
            if role == 'ASSISTANT' and len(content) > 500:
                should_summarize = True
            elif role == 'USER' and len(content) > 500:
                should_summarize = True
                
            if should_summarize and llm_client is not None:
                try:
                    logger.info(f"📝 Summarizing assistant message ({len(content)} chars) for LLM context...")
                    summarized = llm_client.summarize_text(content)
                    content = summarized
                    logger.info(f"✅ Summarized to {len(content)} chars for LLM")
                except Exception as e:
                    logger.warning(f"⚠️ Summarization failed, using original: {e}")
            
            formatted_parts.append(f"{role}: {content}")
        
        return "\n".join(formatted_parts)



@st.cache_resource
def get_chat_history_manager(schema_name: str = 'public') -> ChatHistoryManager:
    """
    Get chat history manager instance.
    Cached per schema to avoid repeated table validation queries.
    
    Args:
        schema_name: Schema to use for chat history (default: 'public')
    
    Returns:
        ChatHistoryManager: Chat history manager instance
    """
    return ChatHistoryManager(schema_name=schema_name)
