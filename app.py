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

# Load custom CSS for modern UI
def load_custom_css():
    """Load custom CSS styling for the application."""
    css_file = Path(__file__).parent / "styles" / "custom.css"
    if css_file.exists():
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    else:
        logger.warning(f"Custom CSS file not found at {css_file}")
    
    # Additional inline CSS for specific customizations
    st.markdown("""
        <style>
        /* Additional custom styling */
        .user-status-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        .status-connected { background-color: #10b981; }
        .status-disconnected { background-color: #ef4444; }
        
        /* Quick action button grid */
        .quick-actions {
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
            margin-bottom: 1rem;
        }
        
        /* Enhanced thinking state */
        .thinking-indicator {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 1rem;
            background: rgba(79, 70, 229, 0.05);
            border-radius: 0.75rem;
            border-left: 4px solid #4f46e5;
        }
        </style>
    """, unsafe_allow_html=True)

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
    # ═══ HERO SECTION ═══
    st.markdown("""
        <div style='text-align: center; padding: 2.5rem 0 2rem 0;'>
            <h1 style='
                font-size: 3.5rem; 
                font-weight: 700; 
                margin-bottom: 0.8rem;
                background: linear-gradient(135deg, #4f46e5 0%, #14b8a6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                animation: fadeIn 0.6s ease-out;
            '>
                🤖 Text-to-SQL Assistant
            </h1>
            <p style='
                font-size: 1.25rem; 
                color: #6b7280; 
                margin-bottom: 0.5rem;
                animation: fadeIn 0.8s ease-out;
            '>
                Transform questions into insights • No SQL required • Instant answers ✨
            </p>
            <p style='
                font-size: 0.95rem; 
                color: #9ca3af;
                animation: fadeIn 1s ease-out;
            '>
                Ask anything about your data in plain English
            </p>
        </div>
        
        <style>
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(15px); }
                to { opacity: 1; transform: translateY(0); }
            }
        </style>
    """, unsafe_allow_html=True)
    
    # ═══ DATABASE STATUS (Compact) ═══
    col_status, col_test = st.columns([4, 1])
    
    with col_status:
        if st.session_state.aiven_db_status == 'connected':
            st.success("🟢 **Database Connected** • Ready to query", icon="✅")
        elif st.session_state.aiven_db_status == 'disconnected':
            st.error("🔴 **Database Disconnected** • Check configuration", icon="⚠️")
        else:
            st.info("🟡 **Status Unknown** • Test connection", icon="ℹ️")
    
    with col_test:
        if st.button("🔄 Test", use_container_width=True, help="Test database connection"):
            with st.spinner("Testing..."):
                status, message, details = check_database_connection()
                st.session_state.aiven_db_status = status
                st.session_state.aiven_db_message = message
                st.session_state.aiven_last_check = details.get('timestamp', '')
                if details:
                    st.session_state.aiven_db_details = details
                st.rerun()
    
    st.divider()
    
    # ═══ USER AUTHENTICATION ═══
    col1, col2 = st.columns([3, 1])
    
    with col1:
        user_id = st.text_input(
            "👤 User ID",
            placeholder="Enter your username (e.g., john_doe)",
            help="Your unique identifier for project isolation"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        use_public = st.checkbox("🔓 Public", help="Use public schema")
    
    if use_public:
        st.info("📖 Using public schema • Shared workspace")
        if st.button("🚀 Connect to Public", type="primary", use_container_width=True):
            st.session_state.user_id = "public"
            st.session_state.active_schema = "public"
            st.session_state.current_project_name = "Public Schema"
            st.rerun()
        return
    
    if not user_id:
        st.markdown("""
            <div style='
                text-align: center; 
                padding: 2.5rem 2rem; 
                background: linear-gradient(135deg, rgba(99, 102, 241, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%);
                border-radius: 1rem;
                margin: 2rem 0;
            '>
                <h3 style='color: #818cf8; margin-bottom: 1rem;'>👋 Welcome! Let's Get Started</h3>
                <p style='color: #94a3b8; font-size: 1.05rem;'>
                    Enter your User ID above to create or access your projects
                </p>
            </div>
        """, unsafe_allow_html=True)
        return
    
    # Fetch existing projects
    project_manager = get_project_manager()
    projects = project_manager.list_user_projects(user_id)
    
    # Welcome message
    st.markdown(f"### 👋 Welcome back, **{user_id}**!")
    st.markdown("")
    
    # Two-column layout
    col_left, col_right = st.columns([3, 2])
    
    # LEFT: Load Existing Project
    with col_left:
        st.markdown("#### 📂 Your Projects")
        
        if projects:
            # Display projects as modern styled cards
            for idx, project in enumerate(projects):
                # Glassmorphic project card
                with st.container():
                    st.markdown(f"""
                        <div class='glass-card' style='
                            background: rgba(255, 255, 255, 0.6);
                            backdrop-filter: blur(10px);
                            border: 1px solid rgba(79, 70, 229, 0.1);
                            border-radius: 1rem;
                            padding: 1.5rem;
                            margin-bottom: 1.25rem;
                            transition: all 0.3s ease;
                        '>
                            <div style='display: flex; align-items: center; margin-bottom: 0.75rem;'>
                                <span style='font-size: 2rem; margin-right: 0.75rem;'>📁</span>
                                <h3 style='margin: 0; color: #111827; font-size: 1.3rem;'>{project['display_name']}</h3>
                            </div>
                            <div style='display: flex; gap: 1.5rem; margin-bottom: 1rem; flex-wrap: wrap;'>
                                <span style='color: #6b7280; font-size: 0.9rem;'>
                                    <strong style='color: #374151;'>{project['table_count']}</strong> tables
                                </span>
                                <span style='color: #6b7280; font-size: 0.9rem;'>
                                    <strong style='color: #374151;'>{project['total_rows']:,}</strong> rows
                                </span>
                                <span style='color: #9ca3af; font-size: 0.85rem;'>
                                    Created {project['created_at']}
                                </span>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    # Action buttons
                    col_btn1, col_btn2, col_btn3 = st.columns([2, 1, 1])
                    
                    with col_btn1:
                        if st.button(
                            "🚀 Open Project",
                            key=f"load_{idx}",
                            type="primary",
                            use_container_width=True
                        ):
                            st.session_state.user_id = user_id
                            st.session_state.active_schema = project['schema_name']
                            st.session_state.current_project_name = project['display_name']
                            st.rerun()
                    
                    with col_btn2:
                        if st.button("📋 Details", key=f"details_{idx}", use_container_width=True):
                            st.session_state[f'show_details_{idx}'] = not st.session_state.get(f'show_details_{idx}', False)
                    
                    with col_btn3:
                        if st.button("🗑️", key=f"delete_{idx}", help="Delete project", use_container_width=True):
                            st.session_state[f'confirm_delete_{idx}'] = True
                    
                    # Show details if toggled
                    if st.session_state.get(f'show_details_{idx}', False):
                        st.markdown(f"""
                        <div style='background: rgba(79, 70, 229, 0.08); padding: 1rem; border-radius: 0.5rem; margin-top: 0.5rem;'>
                            <p style='color: #374151; margin: 0;'><strong>Schema:</strong> <code style='background: #f3f4f6; padding: 0.2rem 0.5rem; border-radius: 0.25rem; color: #4f46e5;'>{project['schema_name']}</code></p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Confirmation dialog for deletion
                    if st.session_state.get(f'confirm_delete_{idx}', False):
                        st.warning(f"⚠️ **Permanently delete** {project['display_name']}? All data will be lost!")
                        col_confirm1, col_confirm2 = st.columns(2)
                        with col_confirm1:
                            if st.button("✅ Confirm Delete", key=f"confirm_yes_{idx}", type="primary"):
                                try:
                                    project_manager.delete_project(project['schema_name'])
                                    st.success(f"✅ Deleted {project['display_name']}")
                                    st.session_state[f'confirm_delete_{idx}'] = False
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        with col_confirm2:
                            if st.button("❌ Cancel", key=f"confirm_no_{idx}"):
                                st.session_state[f'confirm_delete_{idx}'] = False
                                st.rerun()
        else:
            # Empty state with visual design
            st.markdown("""
                <div style='
                    text-align: center;
                    padding: 3rem 2rem;
                    background: rgba(100, 116, 139, 0.1);
                    border: 2px dashed #475569;
                    border-radius: 1rem;
                '>
                    <div style='font-size: 3.5rem; margin-bottom: 1rem;'>📂</div>
                    <h4 style='color: #94a3b8; margin-bottom: 0.5rem;'>No Projects Yet</h4>
                    <p style='color: #64748b;'>Create your first project to get started →</p>
                </div>
            """, unsafe_allow_html=True)
    
    # RIGHT: Create New Project
    with col_right:
        st.markdown("#### ➕ Create New Project")
        
        # Glassmorphic container
        st.markdown("""
            <div class='glass-card' style='
                background: rgba(79, 70, 229, 0.05);
                backdrop-filter: blur(10px);
                border: 1.5px solid rgba(79, 70, 229, 0.2);
                border-radius: 1rem;
                padding: 1.5rem;
                margin-bottom: 1rem;
            '>
        """, unsafe_allow_html=True)
        
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
                            
                            st.balloons()
                            st.rerun()
                        else:
                            st.error(f"❌ Failed to ingest data: {msg}")
                            # Clean up schema on failure
                            project_manager.delete_project(schema_name)
                            
                except Exception as e:
                    st.error(f"❌ Error creating project: {e}")
                    logger.error(f"Project creation error: {e}")
        
        # Close styled container
        st.markdown("</div>", unsafe_allow_html=True)


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
    Process user question through the complete pipeline WITH INTENT ROUTING.
    Routes to either SQL pipeline or general chat based on intent classification.
    
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
        
        # Get chat history for context (with automatic summarization)
        chat_history = chat_manager.format_history_for_llm(st.session_state.session_id, llm_client=llm_client)
        
        # ═══════════════════════════════════════════════════════════
        # STEP 1: INTENT CLASSIFICATION (The Router)
        # ═══════════════════════════════════════════════════════════
        # Enhanced thinking state with glassmorphism
        with st.spinner():
            st.markdown("""
                <div class='thinking-indicator'>
                    <span style='font-size: 1.5rem;'>🔍</span>
                    <span style='color: #4f46e5; font-weight: 600;'>Understanding your request...</span>
                </div>
            """, unsafe_allow_html=True)
            intent = llm_client.classify_intent(user_question, chat_history)
        
        # Display intent badge
        if intent == "GENERAL_CHAT":
            st.info("💬 General Conversation Mode")
        else:
            st.info("🗄️ Database Query Mode")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: CONDITIONAL EXECUTION
        # ═══════════════════════════════════════════════════════════
        
        # Route A: GENERAL CHAT (no database needed)
        if intent == "GENERAL_CHAT":
            with st.spinner("💭 Thinking..."):
                answer = llm_client.general_chat(user_question, chat_history)
            
            # Store in chat history
            chat_manager.insert_message(st.session_state.session_id, "user", user_question, llm_client=llm_client)
            chat_manager.insert_message(st.session_state.session_id, "assistant", answer, llm_client=llm_client)
            
            return answer
        
        # Route B: SQL PIPELINE (needs database)
        # Continue with existing SQL processing logic...
        
        # Generate SQL query with enhanced UI feedback
        with st.spinner():
            st.markdown("""
                <div class='thinking-indicator'>
                    <span style='font-size: 1.5rem;'>🤖</span>
                    <span style='color: #4f46e5; font-weight: 600;'>Generating SQL query...</span>
                </div>
            """, unsafe_allow_html=True)
            sql_query = llm_client.text_to_sql(
                user_question=user_question,
                schema=schema,
                chat_history=chat_history
            )
        
        # Display generated SQL in expander (collapsed by default for clean UX)
        with st.expander("📝 View SQL Query", expanded=False):
            st.code(sql_query, language="sql")
            st.caption("💡 Power users: This is the SQL query generated from your question")
        
        # Validate SQL
        is_valid, validation_error = validate_sql(sql_query)
        
        if not is_valid:
            error_msg = f"⚠️ SQL Validation Failed: {validation_error}"
            st.error(error_msg)
            
            # Store in chat history
            chat_manager.insert_message(st.session_state.session_id, "user", user_question, llm_client=llm_client)
            chat_manager.insert_message(st.session_state.session_id, "assistant", error_msg, llm_client=llm_client)
            
            return error_msg
        
        # Execute SQL (with schema context) and enhanced feedback
        with st.spinner():
            st.markdown("""
                <div class='thinking-indicator'>
                    <span style='font-size: 1.5rem;'>💾</span>
                    <span style='color: #14b8a6; font-weight: 600;'>Executing query...</span>
                </div>
            """, unsafe_allow_html=True)
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
        
        # Display results in expander with enhanced data presentation
        with st.expander("📊 Query Results", expanded=True):
            if results:
                df = pd.DataFrame(results, columns=column_names)
                st.dataframe(
                    df, 
                    use_container_width=True,
                    height=min(400, len(df) * 35 + 38)  # Dynamic height
                )
                st.caption(f"✅ {len(results)} row(s) returned")
                
                # Auto-chart generation for small datasets
                if len(results) <= 10 and len(results) > 0:
                    try:
                        # Check if we have exactly 2 columns (good for bar chart)
                        if len(column_names) == 2:
                            st.markdown("#### 📊 Auto-Generated Visualization")
                            # Assume first column is label, second is value
                            import plotly.express as px
                            fig = px.bar(
                                df,
                                x=column_names[0],
                                y=column_names[1],
                                labels={column_names[0]: column_names[0], column_names[1]: column_names[1]},
                                color_discrete_sequence=['#4f46e5']
                            )
                            fig.update_layout(
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor='rgba(0,0,0,0)',
                                font_color='#374151'
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        # Check for time-series data
                        elif any('date' in col.lower() or 'time' in col.lower() for col in column_names):
                            st.markdown("#### 📈 Time Series Visualization")
                            date_col = next((col for col in column_names if 'date' in col.lower() or 'time' in col.lower()), None)
                            if date_col and len(column_names) == 2:
                                value_col = [col for col in column_names if col != date_col][0]
                                import plotly.express as px
                                fig = px.line(
                                    df,
                                    x=date_col,
                                    y=value_col,
                                    markers=True,
                                    color_discrete_sequence=['#14b8a6']
                                )
                                fig.update_layout(
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    font_color='#374151'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                    except Exception as chart_error:
                        logger.debug(f"Could not auto-generate chart: {chart_error}")
                        # Silent fail - charts are optional enhancement
            else:
                st.info("No results found even after retry")
        
        # Generate descriptive narrative of top 10 rows
        data_description = None
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
        
        # Generate natural language answer with enhanced feedback
        with st.spinner():
            st.markdown("""
                <div class='thinking-indicator'>
                    <span style='font-size: 1.5rem;'>✨</span>
                    <span style='color: #4f46e5; font-weight: 600;'>Crafting your answer...</span>
                </div>
            """, unsafe_allow_html=True)
            answer = llm_client.result_to_english(
                user_question=user_question,
                sql_query=sql_query,
                sql_result=results,
                column_names=column_names
            )
        
        # Combine descriptive narrative with answer for chat history
        full_response = ""
        if data_description:
            full_response = f"### 📖 Data Overview\n\n{data_description}\n\n---\n\n### Summary\n\n{answer}"
        else:
            full_response = answer
        
        # Store in chat history (with descriptive narrative included)
        chat_manager.insert_message(st.session_state.session_id, "user", user_question, llm_client=llm_client)
        chat_manager.insert_message(st.session_state.session_id, "assistant", full_response, llm_client=llm_client)
        
        # Return full response for display (includes Data Overview + Summary)
        return full_response
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        st.error(error_msg)
        return error_msg


def show_chat_interface():
    """Display chat interface for active project."""
    
    # ═══ ENHANCED SIDEBAR WITH USER STATUS ═══
    with st.sidebar:
        # User status at the top
        user_status = "connected" if st.session_state.user_id else "disconnected"
        status_icon = "🟢" if user_status == "connected" else "🔴"
        status_text = "Connected" if user_status == "connected" else "Disconnected"
        
        st.markdown(f"""
            <div class='glass-card' style='
                background: rgba(255, 255, 255, 0.6);
                backdrop-filter: blur(10px);
                padding: 1rem;
                border-radius: 0.75rem;
                margin-bottom: 1rem;
                border: 1px solid rgba(79, 70, 229, 0.1);
            '>
                <div style='display: flex; align-items: center; margin-bottom: 0.5rem;'>
                    <span class='user-status-indicator status-{user_status}'></span>
                    <span style='font-size: 0.85rem; color: #6b7280; font-weight: 600;'>{status_icon} {status_text}</span>
                </div>
                <p style='margin: 0; color: #111827; font-size: 0.9rem;'>
                    <strong>User:</strong> {st.session_state.user_id}
                </p>
            </div>
        """, unsafe_allow_html=True)
        
        # Project info with modern styling
        st.markdown(f"""
            <div class='glass-card' style='
                background: linear-gradient(135deg, rgba(79, 70, 229, 0.1) 0%, rgba(20, 184, 166, 0.1) 100%);
                backdrop-filter: blur(10px);
                padding: 1rem;
                border-radius: 0.75rem;
                margin-bottom: 1rem;
                border: 1px solid rgba(79, 70, 229, 0.2);
            '>
                <h3 style='margin: 0; color: #111827; font-size: 1.1rem;'>
                    📁 {st.session_state.current_project_name}
                </h3>
                <p style='margin: 0.5rem 0 0 0; color: #6b7280; font-size: 0.85rem;'>
                    Schema: <code style='background: rgba(255, 255, 255, 0.6); padding: 0.2rem 0.4rem; border-radius: 0.25rem; color: #4f46e5;'>{st.session_state.active_schema}</code>
                </p>
            </div>
        """, unsafe_allow_html=True)
        
        # ═══ PROJECT SWITCHER ═══
        st.markdown("#### 🔄 Project Switcher")
        try:
            project_manager = get_project_manager()
            all_projects = project_manager.list_user_projects(st.session_state.user_id)
            
            if len(all_projects) > 1:
                project_names = [p['display_name'] for p in all_projects]
                current_index = next(
                    (i for i, p in enumerate(all_projects) if p['schema_name'] == st.session_state.active_schema),
                    0
                )
                
                selected_project = st.selectbox(
                    "Switch to:",
                    project_names,
                    index=current_index,
                    help="Switch between your projects"
                )
                
                # Check if user selected a different project
                selected_idx = project_names.index(selected_project)
                if all_projects[selected_idx]['schema_name'] != st.session_state.active_schema:
                    if st.button("🚀 Switch Project", use_container_width=True, type="primary"):
                        st.session_state.active_schema = all_projects[selected_idx]['schema_name']
                        st.session_state.current_project_name = all_projects[selected_idx]['display_name']
                        st.session_state.messages = []
                        st.session_state.schema_text = None
                        st.session_state.db_connected = False
                        st.rerun()
            else:
                st.info("Only one project available")
        except Exception as e:
            st.warning(f"Could not load projects: {e}")
        
        st.divider()
        
        if st.button("← Back to Projects", use_container_width=True):
            st.session_state.active_schema = None
            st.session_state.current_project_name = None
            st.session_state.messages = []
            st.session_state.schema_text = None
            st.session_state.db_connected = False
            st.rerun()
        
        st.divider()
        
        # Project Analytics
        st.markdown("#### 📊 Analytics")
        
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
            
            st.metric("Queries", len(user_messages))
            
        except Exception as e:
            st.warning(f"Could not load analytics: {e}")
        
        st.divider()
        
        # Database connection
        st.markdown("#### ⚙️ Configuration")
        
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
        st.markdown("#### 📋 Schema")
        
        if st.session_state.schema_text:
            edited_schema = st.text_area(
                "Edit schema if needed:",
                value=st.session_state.schema_text,
                height=250,
                help="Manually edit the schema for additional context"
            )
            
            if st.button("💾 Update Schema", use_container_width=True):
                st.session_state.schema_text = edited_schema
                st.success("Schema updated!")
        else:
            st.info("Load schema to view/edit")
        
        st.divider()
        
        # Session controls
        st.markdown("#### 🗂️ Session")
        st.caption(f"ID: {st.session_state.session_id[:8]}...")
        
        if st.button("🗑️ Clear Chat", use_container_width=True):
            try:
                chat_manager = get_chat_history_manager(schema_name=st.session_state.active_schema)
                chat_manager.clear_session_history(st.session_state.session_id)
                st.session_state.messages = []
                st.success("Chat cleared!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
    
    # ═══ MAIN CHAT INTERFACE ═══
    # Modern header
    st.markdown("""
        <div style='margin-bottom: 1.5rem;'>
            <h1 style='
                font-size: 2.5rem;
                margin-bottom: 0.5rem;
                background: linear-gradient(135deg, #4f46e5 0%, #14b8a6 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            '>
                💬 Chat with Your Data
            </h1>
            <p style='color: #6b7280; font-size: 1.1rem;'>
                Ask questions about <strong style='color: #4f46e5;'>{st.session_state.current_project_name}</strong> in plain English
            </p>
        </div>
    """, unsafe_allow_html=True)
    
    # ═══ QUICK ACTION BUTTONS ═══
    if st.session_state.db_connected and st.session_state.schema_text:
        st.markdown("### 🎯 Quick Actions")
        
        # Extract table names from schema for suggestions
        try:
            import re
            table_matches = re.findall(r'Table: (\w+)', st.session_state.schema_text)
            sample_table = table_matches[0] if table_matches else "your_table"
        except:
            sample_table = "your_table"
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📊 Show top 5 rows", use_container_width=True, help="Quick peek at data"):
                st.session_state.quick_query = f"Show me the first 5 rows from {sample_table}"
                st.rerun()
        
        with col2:
            if st.button("🔢 Count records", use_container_width=True, help="Total row count"):
                st.session_state.quick_query = f"How many total records are in {sample_table}?"
                st.rerun()
        
        with col3:
            if st.button("📋 Column names", use_container_width=True, help="See all columns"):
                st.session_state.quick_query = "What are all the column names in the database?"
                st.rerun()
        
        with col4:
            if st.button("📈 Summary stats", use_container_width=True, help="Basic statistics"):
                st.session_state.quick_query = f"Give me summary statistics for {sample_table}"
                st.rerun()
        
        st.divider()
    
    # Check connection status
    if not st.session_state.db_connected:
        st.markdown("""
            <div class='glass-card' style='
                background: rgba(245, 158, 11, 0.1);
                backdrop-filter: blur(10px);
                padding: 1.5rem;
                border-radius: 1rem;
                border-left: 4px solid #f59e0b;
                text-align: center;
            '>
                <h3 style='color: #d97706; margin-bottom: 0.5rem;'>⚠️ Schema Not Loaded</h3>
                <p style='color: #92400e; margin: 0;'>Please load the database schema using the sidebar before asking questions.</p>
                <p style='color: #92400e; margin-top: 0.5rem;'>👈 Click <strong>'Load Schema'</strong> in the sidebar to get started.</p>
            </div>
        """, unsafe_allow_html=True)
        return
    
    # Display chat messages with avatars
    for message in st.session_state.messages:
        avatar = "👤" if message["role"] == "user" else "🤖"
        with st.chat_message(message["role"], avatar=avatar):
            st.markdown(message["content"])
    
    # Chat input (fixed at bottom with CSS)
    # Check for quick query
    if 'quick_query' in st.session_state and st.session_state.quick_query:
        prompt = st.session_state.quick_query
        st.session_state.quick_query = None
    else:
        prompt = st.chat_input("✨ Ask a question about your database...")
    
    if prompt:
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
        page_title="✨ Text-to-SQL Chatbot",
        page_icon="🤖",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Load custom CSS for modern UI styling
    load_custom_css()
    
    # Initialize session state
    initialize_session_state()
    
    # Route based on active schema
    if st.session_state.active_schema is None:
        show_project_dashboard()
    else:
        show_chat_interface()


if __name__ == "__main__":
    main()
