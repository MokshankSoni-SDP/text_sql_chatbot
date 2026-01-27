"""
Main Streamlit application for Multi-Tenant Text-to-SQL chatbot.
Supports project-based data isolation with schema sandboxing.
"""

import streamlit as st
import uuid
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import tempfile
import os

# Import custom modules
from modules.project_manager import get_project_manager
from modules.data_ingestion import get_data_ingestor
from modules.schema_extractor import get_enriched_database_schema
from modules.chat_history import get_chat_history_manager
from modules.llm_client import get_llm_client
from modules.sql_validator import validate_sql
from modules.sql_executor import execute_sql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    
    if 'user_id' not in st.session_state:
        st.session_state.user_id = None
    
    if 'active_schema' not in st.session_state:
        st.session_state.active_schema = None
    
    if 'current_project_name' not in st.session_state:
        st.session_state.current_project_name = None
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    
    if 'schema_text' not in st.session_state:
        st.session_state.schema_text = None
    
    if 'db_connected' not in st.session_state:
        st.session_state.db_connected = False
    
    if 'data_preview' not in st.session_state:
        st.session_state.data_preview = None
    
    if 'uploaded_df' not in st.session_state:
        st.session_state.uploaded_df = None
    
    if 'temp_file_path' not in st.session_state:
        st.session_state.temp_file_path = None
    
    # Aiven database connection status
    if 'aiven_db_status' not in st.session_state:
        st.session_state.aiven_db_status = 'unknown'  # 'unknown', 'connected', 'disconnected'
    
    if 'aiven_db_message' not in st.session_state:
        st.session_state.aiven_db_message = ''
    
    if 'aiven_last_check' not in st.session_state:
        st.session_state.aiven_last_check = None


def check_database_connection():
    """
    Test connection to Aiven PostgreSQL database.
    
    Returns:
        tuple: (status, message, details_dict)
            status: 'connected' or 'disconnected'
            message: Human-readable status message
            details_dict: Additional connection information
    """
    try:
        from modules.db_connection import get_db_instance
        import os
        from datetime import datetime
        
        # Get database instance
        db = get_db_instance()
        
        # Test with simple query
        result = db.execute_query("SELECT 1 as test, version() as pg_version;", fetch=True)
        
        if result:
            # Extract PostgreSQL version
            pg_version = result[0][1] if len(result[0]) > 1 else "Unknown"
            
            # Get connection details from environment
            db_host = os.getenv('DB_HOST', 'localhost')
            db_name = os.getenv('DB_NAME', 'defaultdb')
            db_port = os.getenv('DB_PORT', '5432')
            
            # Determine if using SSL
            is_aiven = 'aivencloud.com' in db_host
            ssl_status = "SSL Enabled (Aiven)" if is_aiven else "Local Connection"
            
            details = {
                'host': db_host,
                'database': db_name,
                'port': db_port,
                'ssl_status': ssl_status,
                'pg_version': pg_version.split(',')[0] if ',' in pg_version else pg_version[:50],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            message = f"Connected to {db_host}:{db_port}/{db_name}"
            
            return 'connected', message, details
        else:
            return 'disconnected', 'Query returned no results', {}
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Database connection test failed: {error_msg}")
        return 'disconnected', f"Connection failed: {error_msg}", {}


def show_project_dashboard():
    """Display project selection/creation dashboard."""
    st.title("📂 Text-to-SQL Projects")
    st.markdown("Manage your data projects with isolated schemas")
    
    # ========== DATABASE CONNECTION STATUS SECTION ==========
    st.divider()
    
    # Connection status header
    col_status_title, col_status_button = st.columns([3, 1])
    
    with col_status_title:
        st.subheader("🔌 Database Connection Status")
    
    with col_status_button:
        if st.button("🔄 Test Connection", use_container_width=True):
            with st.spinner("Testing connection to Aiven..."):
                status, message, details = check_database_connection()
                st.session_state.aiven_db_status = status
                st.session_state.aiven_db_message = message
                st.session_state.aiven_last_check = details.get('timestamp', '')
                
                # Store details for display
                if details:
                    st.session_state.aiven_db_details = details
    
    # Display connection status
    if st.session_state.aiven_db_status == 'connected':
        st.success(f"🟢 **Connected** - {st.session_state.aiven_db_message}")
        
        # Show connection details in expander
        if hasattr(st.session_state, 'aiven_db_details'):
            details = st.session_state.aiven_db_details
            with st.expander("📋 Connection Details", expanded=False):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Host", details.get('host', 'N/A'))
                    st.caption(f"Port: {details.get('port', 'N/A')}")
                
                with col2:
                    st.metric("Database", details.get('database', 'N/A'))
                    st.caption(details.get('ssl_status', 'N/A'))
                
                with col3:
                    st.metric("Status", "Online")
                    st.caption(f"Last check: {details.get('timestamp', 'N/A')}")
                
                if 'pg_version' in details:
                    st.info(f"**PostgreSQL Version:** {details['pg_version']}")
    
    elif st.session_state.aiven_db_status == 'disconnected':
        st.error(f"🔴 **Disconnected** - {st.session_state.aiven_db_message}")
        st.warning("⚠️ Please check your database credentials in `.env` file and ensure the Aiven service is running.")
    
    else:  # unknown
        st.info("🟡 **Connection status unknown** - Click 'Test Connection' to verify database connectivity")
    
    st.divider()
    # ========== END DATABASE CONNECTION STATUS ==========
    
    # User ID input
    col1, col2 = st.columns([2, 1])
    with col1:
        user_id = st.text_input(
            "👤 User ID",
            placeholder="Enter your user ID (e.g., john_doe)",
            help="Your unique identifier for project isolation"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        use_public = st.checkbox("Use legacy public schema", help="Access the original public schema")
    
    if use_public:
        st.info("🔓 Using legacy public schema (no user isolation)")
        if st.button("Connect to Public Schema", type="primary"):
            st.session_state.user_id = "public"
            st.session_state.active_schema = "public"
            st.session_state.current_project_name = "Public Schema"
            st.rerun()
        return
    
    if not user_id:
        st.info("👆 Enter your User ID to view or create projects")
        return
    
    # Fetch existing projects
    project_manager = get_project_manager()
    projects = project_manager.list_user_projects(user_id)
    
    # Two-column layout
    col_left, col_right = st.columns(2)
    
    # LEFT: Load Existing Project
    with col_left:
        st.subheader("📂 Load Existing Project")
        
        if projects:
            # Display projects as cards
            for idx, project in enumerate(projects):
                with st.container():
                    st.markdown(f"### {project['display_name']}")
                    
                    # Project metadata
                    col_meta1, col_meta2, col_meta3 = st.columns(3)
                    with col_meta1:
                        st.metric("Tables", project['table_count'])
                    with col_meta2:
                        st.metric("Rows", f"{project['total_rows']:,}")
                    with col_meta3:
                        st.caption(f"Created: {project['created_at']}")
                    
                    # Action buttons
                    col_btn1, col_btn2 = st.columns(2)
                    with col_btn1:
                        if st.button(f"📖 Load", key=f"load_{idx}", use_container_width=True):
                            st.session_state.user_id = user_id
                            st.session_state.active_schema = project['schema_name']
                            st.session_state.current_project_name = project['display_name']
                            st.rerun()
                    
                    with col_btn2:
                        if st.button(f"🗑️ Delete", key=f"delete_{idx}", use_container_width=True):
                            st.session_state[f'confirm_delete_{idx}'] = True
                    
                    # Confirmation dialog for deletion
                    if st.session_state.get(f'confirm_delete_{idx}', False):
                        st.warning(f"⚠️ Delete **{project['display_name']}**? This will permanently delete all data!")
                        col_confirm1, col_confirm2 = st.columns(2)
                        with col_confirm1:
                            if st.button("✅ Confirm Delete", key=f"confirm_yes_{idx}", type="primary"):
                                try:
                                    project_manager.delete_project(project['schema_name'])
                                    st.success(f"✅ Deleted {project['display_name']}")
                                    st.session_state[f'confirm_delete_{idx}'] = False
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error deleting project: {e}")
                        with col_confirm2:
                            if st.button("❌ Cancel", key=f"confirm_no_{idx}"):
                                st.session_state[f'confirm_delete_{idx}'] = False
                                st.rerun()
                    
                    st.divider()
        else:
            st.info("No projects found. Create a new one! →")
    
    # RIGHT: Create New Project
    with col_right:
        st.subheader("🆕 Create New Project")
        
        project_name = st.text_input(
            "Project Name",
            placeholder="e.g., sales_data",
            help="Only letters, numbers, and underscores"
        )
        
        uploaded_file = st.file_uploader(
            "Upload Data File",
            type=['csv', 'xlsx', 'xls', 'json'],
            help="CSV, Excel, or JSON file"
        )
        
        # Data preview section
        if uploaded_file is not None:
            try:
                # Save to temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = tmp_file.name
                
                # Read file based on type
                file_ext = Path(uploaded_file.name).suffix.lower()
                
                if file_ext == '.csv':
                    df_preview = pd.read_csv(tmp_path, nrows=100)
                elif file_ext in ['.xlsx', '.xls']:
                    df_preview = pd.read_excel(tmp_path, nrows=100)
                elif file_ext == '.json':
                    try:
                        df_preview = pd.read_json(tmp_path)
                    except:
                        df_preview = pd.read_json(tmp_path, lines=True)
                    df_preview = df_preview.head(100)
                
                # Data preview
                with st.expander("📊 Data Preview", expanded=True):
                    st.write(f"**Shape:** {df_preview.shape[0]} rows × {df_preview.shape[1]} columns")
                    st.dataframe(df_preview.head(10), use_container_width=True)
                    
                    # Column info
                    st.write("**Column Types:**")
                    col_types = pd.DataFrame({
                        'Column': df_preview.columns,
                        'Type': df_preview.dtypes.astype(str),
                        'Nulls': df_preview.isnull().sum().values
                    })
                    st.dataframe(col_types, use_container_width=True)
                
                # Store for later use
                st.session_state.temp_file_path = tmp_path
                st.session_state.uploaded_df = df_preview
                
            except Exception as e:
                st.error(f"Error reading file: {e}")
                st.session_state.temp_file_path = None
                st.session_state.uploaded_df = None
        
        # Create project button
        if st.button("🚀 Create Project", type="primary", use_container_width=True):
            if not project_name:
                st.error("Please enter a project name")
            elif not uploaded_file:
                st.error("Please upload a data file")
            else:
                try:
                    with st.spinner("Creating project and ingesting data..."):
                        # Create schema
                        schema_name = project_manager.create_project(user_id, project_name)
                        logger.info(f"Created schema: {schema_name}")
                        
                        # Ingest file
                        ingestor = get_data_ingestor()
                        file_ext = Path(uploaded_file.name).suffix.lower()
                        
                        if file_ext == '.csv':
                            success, msg = ingestor.ingest_csv(st.session_state.temp_file_path, schema_name)
                        elif file_ext in ['.xlsx', '.xls']:
                            success, msg = ingestor.ingest_excel(st.session_state.temp_file_path, schema_name)
                        elif file_ext == '.json':
                            success, msg = ingestor.ingest_json(st.session_state.temp_file_path, schema_name)
                        
                        if success:
                            st.success(f"✅ Project created successfully!\n\n{msg}")
                            
                            # Set active schema and redirect
                            st.session_state.user_id = user_id
                            st.session_state.active_schema = schema_name
                            st.session_state.current_project_name = project_name.replace('_', ' ').title()
                            
                            # Clean up temp file
                            if st.session_state.temp_file_path:
                                try:
                                    os.unlink(st.session_state.temp_file_path)
                                except:
                                    pass
                            
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to ingest data: {msg}")
                            # Clean up schema on failure
                            project_manager.delete_project(schema_name)
                            
                except Exception as e:
                    st.error(f"❌ Error creating project: {e}")
                    logger.error(f"Project creation error: {e}")


def load_schema(schema_name: str):
    """Load enriched database schema for a specific project."""
    try:
        schema = get_enriched_database_schema(schema_name=schema_name)
        st.session_state.schema_text = schema
        st.session_state.db_connected = True
        logger.info(f"✅ Loaded schema for: {schema_name}")
        return True, schema
    except Exception as e:
        st.session_state.db_connected = False
        return False, str(e)


def split_questions(user_input: str) -> list[str]:
    """
    Split multiple questions from a single input.
    Splits by '?' or newline characters.
    
    Args:
        user_input: Raw user input that may contain multiple questions
        
    Returns:
        list[str]: List of individual questions
    """
    import re
    
    questions = []
    parts = re.split(r'[?\n]+', user_input)
    
    for part in parts:
        cleaned = part.strip()
        if cleaned:
            if not cleaned.endswith('?'):
                cleaned += '?'
            questions.append(cleaned)
    
    return questions if questions else [user_input.strip()]


def process_user_question(user_question: str, schema: str, schema_name: str):
    """
    Process user question through the complete pipeline.
    
    Args:
        user_question: User's natural language question
        schema: Database schema text
        schema_name: Active schema name
        
    Returns:
        str: Final answer to display to user
    """
    try:
        # Get chat history manager and LLM client (schema-aware)
        chat_manager = get_chat_history_manager(schema_name=schema_name)
        llm_client = get_llm_client()
        
        # Get chat history for context
        chat_history = chat_manager.format_history_for_llm(st.session_state.session_id)
        
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
            chat_manager.insert_message(st.session_state.session_id, "user", user_question, llm_client=llm_client)
            chat_manager.insert_message(st.session_state.session_id, "assistant", error_msg, llm_client=llm_client)
            
            return error_msg
        
        # Execute SQL (with schema context)
        with st.spinner("💾 Executing query..."):
            success, results, column_names, exec_error = execute_sql(sql_query, schema_name=schema_name)
        
        if not success:
            error_msg = f"❌ Query Execution Failed: {exec_error}"
            st.error(error_msg)
            
            # Store in chat history
            chat_manager.insert_message(st.session_state.session_id, "user", user_question, llm_client=llm_client)
            chat_manager.insert_message(st.session_state.session_id, "assistant", error_msg, llm_client=llm_client)
            
            return error_msg
        
        # Auto-retry if no results
        if success and len(results) == 0:
            st.warning("⚠️ Query returned 0 results. Attempting automatic correction...")
            
            try:
                chat_history = chat_manager.format_history_for_llm(st.session_state.session_id)
                
                with st.spinner("🔄 Retrying with corrected filter values..."):
                    corrected_sql = llm_client.retry_query_on_empty_results(
                        failed_sql=sql_query,
                        user_question=user_question,
                        schema=schema,
                        chat_history=chat_history
                    )
                
                with st.expander("🔧 Corrected SQL Query (Retry)", expanded=True):
                    st.code(corrected_sql, language="sql")
                    st.info("Auto-corrected to use valid filter values from the database")
                
                is_valid_retry, validation_error_retry = validate_sql(corrected_sql)
                
                if is_valid_retry:
                    with st.spinner("💾 Executing corrected query..."):
                        success_retry, results_retry, column_names_retry, exec_error_retry = execute_sql(
                            corrected_sql, schema_name=schema_name
                        )
                    
                    if success_retry:
                        results = results_retry
                        column_names = column_names_retry
                        sql_query = corrected_sql
                        st.success(f"✅ Retry successful! Found {len(results)} result(s)")
                    else:
                        st.error(f"Retry also failed: {exec_error_retry}")
                else:
                    st.error(f"Corrected query also invalid: {validation_error_retry}")
                    
            except Exception as retry_error:
                logger.error(f"Retry failed: {retry_error}")
                st.error(f"Auto-correction failed: {retry_error}")
        
        # Display results in expander
        with st.expander("📊 Query Results", expanded=False):
            if results:
                df = pd.DataFrame(results, columns=column_names)
                st.dataframe(df, use_container_width=True)
                st.caption(f"Rows returned: {len(results)}")
            else:
                st.info("No results found even after retry")
        
        # Generate descriptive narrative of top 10 rows
        if results and len(results) > 0:
            with st.spinner("📝 Describing your data..."):
                try:
                    data_description = llm_client.describe_data_rows(
                        user_question=user_question,
                        sql_result=results,
                        column_names=column_names,
                        max_rows=10
                    )
                    
                    # Display descriptive narrative
                    st.markdown("### 📖 Data Overview")
                    st.markdown(data_description)
                    st.divider()
                    
                except Exception as desc_error:
                    logger.error(f"Error generating data description: {desc_error}")
                    # Continue even if description fails
        
        # Generate natural language answer
        with st.spinner("✨ Generating answer..."):
            answer = llm_client.result_to_english(
                user_question=user_question,
                sql_query=sql_query,
                sql_result=results,
                column_names=column_names
            )
        
        # Store in chat history
        chat_manager.insert_message(st.session_state.session_id, "user", user_question, llm_client=llm_client)
        chat_manager.insert_message(st.session_state.session_id, "assistant", answer, llm_client=llm_client)
        
        return answer
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        st.error(error_msg)
        return error_msg


def show_chat_interface():
    """Display chat interface for active project."""
    
    # Sidebar
    with st.sidebar:
        # Project info
        st.success(f"📂 **{st.session_state.current_project_name}**")
        st.caption(f"Schema: `{st.session_state.active_schema}`")
        
        if st.button("← Back to Projects", use_container_width=True):
            st.session_state.active_schema = None
            st.session_state.current_project_name = None
            st.session_state.messages = []
            st.session_state.schema_text = None
            st.session_state.db_connected = False
            st.rerun()
        
        st.divider()
        
        # Project Analytics
        st.subheader("📊 Project Analytics")
        
        try:
            project_manager = get_project_manager()
            metadata = project_manager.get_project_metadata(st.session_state.active_schema)
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Tables", metadata['table_count'])
            with col2:
                st.metric("Total Rows", f"{metadata['total_rows']:,}")
            
            # Query count (from chat history)
            chat_manager = get_chat_history_manager(schema_name=st.session_state.active_schema)
            messages = chat_manager.get_recent_messages(st.session_state.session_id, limit=1000)
            user_messages = [m for m in messages if m['role'] == 'user']
            
            st.metric("Queries This Session", len(user_messages))
            
        except Exception as e:
            st.warning(f"Could not load analytics: {e}")
        
        st.divider()
        
        # Database connection
        st.subheader("⚙️ Configuration")
        
        if st.button("🔌 Load Schema", use_container_width=True):
            with st.spinner("Loading database schema..."):
                success, result = load_schema(st.session_state.active_schema)
                if success:
                    st.success("✅ Schema loaded!")
                else:
                    st.error(f"❌ Failed: {result}")
        
        if st.session_state.db_connected:
            st.success("🟢 Schema Loaded")
        else:
            st.warning("🔴 Schema Not Loaded")
        
        st.divider()
        
        # Schema editor
        st.subheader("📋 Database Schema")
        
        if st.session_state.schema_text:
            edited_schema = st.text_area(
                "Edit schema if needed:",
                value=st.session_state.schema_text,
                height=300,
                help="You can manually edit the schema to provide additional context"
            )
            
            if st.button("💾 Update Schema", use_container_width=True):
                st.session_state.schema_text = edited_schema
                st.success("Schema updated!")
        else:
            st.info("Load schema to view/edit")
        
        st.divider()
        
        # Session controls
        st.subheader("🗂️ Session")
        st.caption(f"ID: {st.session_state.session_id[:8]}...")
        
        if st.button("🗑️ Clear Chat History", use_container_width=True):
            try:
                chat_manager = get_chat_history_manager(schema_name=st.session_state.active_schema)
                chat_manager.clear_session_history(st.session_state.session_id)
                st.session_state.messages = []
                st.success("Chat history cleared!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
    
    # Main content
    st.title("💬 Text-to-SQL Chatbot")
    st.markdown(f"Ask questions about your **{st.session_state.current_project_name}** data!")
    
    # Check connection status
    if not st.session_state.db_connected:
        st.warning("⚠️ Please load the database schema using the sidebar before asking questions.")
        st.info("👈 Click 'Load Schema' in the sidebar to get started.")
        return
    
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about your database..."):
        # Split into multiple questions if present
        questions = split_questions(prompt)
        
        if len(questions) > 1:
            st.info(f"🔍 Detected {len(questions)} questions. Processing each one separately...")
        
        # Process each question individually
        for idx, question in enumerate(questions, 1):
            # Display user message
            with st.chat_message("user"):
                if len(questions) > 1:
                    st.markdown(f"**Question {idx}/{len(questions)}:** {question}")
                else:
                    st.markdown(question)
            
            st.session_state.messages.append({"role": "user", "content": question})
            
            # Process question and get response
            with st.chat_message("assistant"):
                if len(questions) > 1:
                    st.markdown(f"**Answer {idx}/{len(questions)}:**")
                answer = process_user_question(
                    question, 
                    st.session_state.schema_text,
                    st.session_state.active_schema
                )
                st.markdown(answer)
            
            st.session_state.messages.append({"role": "assistant", "content": answer})
            
            # Add visual separator between Q&A pairs
            if idx < len(questions):
                st.divider()


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
    
    # Route based on active schema
    if st.session_state.active_schema is None:
        show_project_dashboard()
    else:
        show_chat_interface()


if __name__ == "__main__":
    main()
