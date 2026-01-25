"""
Main Streamlit application for Text-to-SQL chatbot.
Integrates all modules to provide a complete chat interface.
"""

import streamlit as st
import uuid
from datetime import datetime

# Import custom modules
from modules.schema_extractor import get_database_schema
from modules.chat_history import get_chat_history_manager
from modules.llm_client import get_llm_client
from modules.sql_validator import validate_sql
from modules.sql_executor import execute_sql


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if 'schema_text' not in st.session_state:
        st.session_state.schema_text = None
    
    if 'db_connected' not in st.session_state:
        st.session_state.db_connected = False


def load_schema():
    """Load database schema."""
    try:
        schema = get_database_schema()
        st.session_state.schema_text = schema
        st.session_state.db_connected = True
        return True, schema
    except Exception as e:
        st.session_state.db_connected = False
        return False, str(e)


def process_user_question(user_question: str, schema: str):
    """
    Process user question through the complete pipeline.
    
    Args:
        user_question: User's natural language question
        schema: Database schema text
        
    Returns:
        str: Final answer to display to user
    """
    try:
        # Get chat history manager and LLM client
        chat_manager = get_chat_history_manager()
        llm_client = get_llm_client()
        
        # Get chat history for context
        chat_history = chat_manager.format_history_for_llm(
            st.session_state.session_id
        )
        
        # Generate SQL query
        with st.spinner("🤖 Generating SQL query..."):
            sql_query = llm_client.text_to_sql(
                user_question=user_question,
                schema=schema,
                chat_history=chat_history
            )
        
        # Display generated SQL in expander
        with st.expander("📝 Generated SQL Query", expanded=False):
            st.code(sql_query, language="sql")
        
        # Validate SQL
        is_valid, validation_error = validate_sql(sql_query)
        
        if not is_valid:
            error_msg = f"⚠️ SQL Validation Failed: {validation_error}"
            st.error(error_msg)
            
            # Store in chat history
            chat_manager.insert_message(
                st.session_state.session_id,
                "user",
                user_question,
                llm_client=llm_client
            )
            chat_manager.insert_message(
                st.session_state.session_id,
                "assistant",
                error_msg,
                llm_client=llm_client
            )
            
            return error_msg
        
        # Execute SQL
        with st.spinner("💾 Executing query..."):
            success, results, column_names, exec_error = execute_sql(sql_query)
        
        if not success:
            error_msg = f"❌ Query Execution Failed: {exec_error}"
            st.error(error_msg)
            
            # Store in chat history
            chat_manager.insert_message(
                st.session_state.session_id,
                "user",
                user_question,
                llm_client=llm_client
            )
            chat_manager.insert_message(
                st.session_state.session_id,
                "assistant",
                error_msg,
                llm_client=llm_client
            )
            
            return error_msg
        
        # Display results in expander
        with st.expander("📊 Query Results", expanded=False):
            if results:
                import pandas as pd
                df = pd.DataFrame(results, columns=column_names)
                st.dataframe(df, use_container_width=True)
                st.caption(f"Rows returned: {len(results)}")
            else:
                st.info("No results found")
        
        # Generate natural language answer
        with st.spinner("✨ Generating answer..."):
            answer = llm_client.result_to_english(
                user_question=user_question,
                sql_query=sql_query,
                sql_result=results,
                column_names=column_names
            )
        
        # Store in chat history (with smart LLM-based summarization)
        chat_manager.insert_message(
            st.session_state.session_id,
            "user",
            user_question,
            llm_client=llm_client
        )
        chat_manager.insert_message(
            st.session_state.session_id,
            "assistant",
            answer,
            llm_client=llm_client  # LLM will summarize if > 300 chars
        )
        
        return answer
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        st.error(error_msg)
        return error_msg


def main():
    """Main application function."""
    st.set_page_config(
        page_title="Text-to-SQL Chatbot",
        page_icon="💬",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state
    initialize_session_state()
    
    # Sidebar
    with st.sidebar:
        st.title("⚙️ Configuration")
        
        # Database connection status
        if st.button("🔌 Connect to Database", use_container_width=True):
            with st.spinner("Connecting to database..."):
                success, result = load_schema()
                if success:
                    st.success("✅ Connected successfully!")
                else:
                    st.error(f"❌ Connection failed: {result}")
        
        # Connection status indicator
        if st.session_state.db_connected:
            st.success("🟢 Database Connected")
        else:
            st.warning("🔴 Not Connected")
        
        st.divider()
        
        # Schema editor
        st.subheader("📋 Database Schema")
        
        if st.session_state.schema_text:
            # Editable schema text area
            edited_schema = st.text_area(
                "Edit schema if needed:",
                value=st.session_state.schema_text,
                height=400,
                help="You can manually edit the schema to provide additional context to the LLM"
            )
            
            if st.button("💾 Update Schema", use_container_width=True):
                st.session_state.schema_text = edited_schema
                st.success("Schema updated!")
        else:
            st.info("Connect to database to load schema")
        
        st.divider()
        
        # Session controls
        st.subheader("🗂️ Session")
        st.caption(f"Session ID: {st.session_state.session_id[:8]}...")
        
        if st.button("🗑️ Clear Chat History", use_container_width=True):
            try:
                chat_manager = get_chat_history_manager()
                chat_manager.clear_session_history(st.session_state.session_id)
                st.session_state.messages = []
                st.success("Chat history cleared!")
                st.rerun()
            except Exception as e:
                st.error(f"Error clearing history: {e}")
    
    # Main content
    st.title("💬 Text-to-SQL Chatbot")
    st.markdown("Ask questions about your database in natural language!")
    
    # Check connection status
    if not st.session_state.db_connected:
        st.warning("⚠️ Please connect to the database using the sidebar before asking questions.")
        st.info("👈 Click 'Connect to Database' in the sidebar to get started.")
        return
    
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about your database..."):
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Add to session messages
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Process question and get response
        with st.chat_message("assistant"):
            answer = process_user_question(prompt, st.session_state.schema_text)
            st.markdown(answer)
        
        # Add assistant response to session messages
        st.session_state.messages.append({"role": "assistant", "content": answer})


if __name__ == "__main__":
    main()
