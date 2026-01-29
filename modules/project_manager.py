"""
Project Manager Module
Manages PostgreSQL schema-based project lifecycle for multi-tenant architecture.
Each user project is isolated in its own schema (proj_{user_id}_{project_name}).
"""

import re
import logging
import os
import pandas as pd
from typing import List, Dict, Optional, Union
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pathlib import Path
import streamlit as st
from .db_connection import get_db_instance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load env for SQLAlchemy
load_dotenv()

class ProjectManager:
    """
    Manages project/schema lifecycle for multi-tenant Text-to-SQL system.
    """
    
    def __init__(self):
        """Initialize project manager with Database Connection and SQLAlchemy Engine."""
        self.db = get_db_instance()
        self.schema_prefix = "proj_"
        
        # Initialize SQLAlchemy Engine for batch processing
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'text_to_sql_chatbot')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        # Create engine
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.engine = create_engine(connection_string)

    def list_user_projects(self, user_id: str) -> List[Dict[str, str]]:
        """List all projects for a specific user."""
        try:
            safe_user_id = self.sanitize_name(user_id)
            pattern = f"{self.schema_prefix}{safe_user_id}_%"
            
            query = """
                SELECT 
                    schema_name,
                    pg_catalog.obj_description(
                        (SELECT oid FROM pg_namespace WHERE nspname = schema_name), 
                        'pg_namespace'
                    ) as description
                FROM information_schema.schemata
                WHERE schema_name LIKE %s
                ORDER BY schema_name;
            """
            
            results = self.db.execute_query(query, (pattern,))
            
            projects = []
            for row in results:
                schema_name = row[0]
                parts = schema_name.split('_', 2)  # Split into max 3 parts
                project_name = parts[2] if len(parts) > 2 else schema_name
                metadata = self.get_project_metadata(schema_name)
                
                projects.append({
                    'schema_name': schema_name,
                    'display_name': project_name.replace('_', ' ').title(),
                    'project_name': project_name,
                    'created_at': metadata.get('created_at', 'Unknown'),
                    'table_count': metadata.get('table_count', 0),
                    'total_rows': metadata.get('total_rows', 0)
                })
            
            return projects
            
        except Exception as e:
            logger.error(f"Error listing projects for user '{user_id}': {e}")
            return []
    
    def create_project(self, user_id: str, project_name: str, data_file: Optional[Union[str, pd.DataFrame]] = None) -> str:
        """
        Create a new project schema and optionally ingest data using robust Batch Processing.
        
        Args:
            user_id: User identifier
            project_name: Project name
            data_file: Optional path to CSV/Excel or DataFrame to upload
            
        Returns:
            str: Created schema name
        """
        conn = None
        try:
            # 1. Sanitize & Construct Name
            safe_user_id = self.sanitize_name(user_id)
            safe_project_name = self.sanitize_name(project_name)
            schema_name = f"{self.schema_prefix}{safe_user_id}_{safe_project_name}"
            
            if len(schema_name) > 63:
                raise ValueError(f"Schema name too long ({len(schema_name)}). Shorten project name.")

            # 2. Commit Schema Creation IMMEDIATELY (Isolation from Data Upload)
            create_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
            self.db.execute_query(create_query, fetch=False)
            logger.info(f"✅ Created/Verified Schema: {schema_name}")

            # 3. Create Metadata Table
            self._create_metadata_table(schema_name)

            # 4. Handle Data Ingestion (if file provided)
            if data_file is not None:
                self.add_file_to_project(schema_name, data_file)
                
            return schema_name

        except Exception as e:
            logger.error(f"Error creating project: {e}")
            raise

    def _create_metadata_table(self, schema_name: str):
        """Create table to track project files."""
        query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.project_tables (
                id SERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER
            );
        """
        self.db.execute_query(query, fetch=False)

    def add_file_to_project(self, schema_name: str, data_file: Union[str, pd.DataFrame, any]) -> str:
        """
        Add a new file/table to an existing project.
        
        Args:
            schema_name: Target schema
            data_file: File path, DataFrame, or UploadedFile
            
        Returns:
            str: Name of the created table
        """
        from .data_ingestion import DataIngestor
        ingestor = DataIngestor()
        
        # Determine file name and load df
        file_name = "unknown"
        df = None
        
        if isinstance(data_file, str):
            file_name = os.path.basename(data_file)
            if data_file.endswith('.csv'):
                df = pd.read_csv(data_file)
            elif data_file.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(data_file)
        elif isinstance(data_file, pd.DataFrame):
            file_name = "dataframe_upload"
            df = data_file
        elif hasattr(data_file, 'name'): # Streamlit UploadedFile
            file_name = data_file.name
            if file_name.endswith('.csv'):
                df = pd.read_csv(data_file)
            else:
                df = pd.read_excel(data_file)
                
        if df is None:
            raise ValueError("Could not process file format")


        # Ensure metadata table exists (for backward compatibility with old projects)
        self._create_metadata_table(schema_name)

        # Sanitize table name from filename
        base_name = os.path.splitext(file_name)[0]
        table_name = ingestor.sanitize_table_name(base_name)
        
        # Ingest with conflict handling
        success, final_table_name, msg = ingestor.ingest_dataframe_with_metadata(
            df, schema_name, table_name
        )
        
        if success:
            # Update metadata
            self._register_table(schema_name, final_table_name, file_name, len(df))
            return final_table_name
        else:
            raise Exception(msg)

    def _register_table(self, schema_name: str, table_name: str, file_name: str, row_count: int):
        """Register a new table in metadata."""
        query = f"""
            INSERT INTO {schema_name}.project_tables (table_name, file_name, row_count)
            VALUES (%s, %s, %s);
        """
        self.db.execute_query(query, (table_name, file_name, row_count), fetch=False)



    
    def delete_project(self, schema_name: str) -> bool:
        """
        Delete a project schema and all its tables.
        
        Args:
            schema_name: Schema name to delete
            
        Returns:
            bool: True if successful
        """
        try:
            if not schema_name.startswith(self.schema_prefix):
                raise ValueError(f"Cannot delete schema '{schema_name}'. Must start with '{self.schema_prefix}'.")
            
            protected_schemas = ['public', 'information_schema', 'pg_catalog', 'pg_toast']
            if schema_name in protected_schemas:
                raise ValueError(f"Cannot delete protected schema: {schema_name}")
            
            if not self.validate_schema_exists(schema_name):
                logger.warning(f"Schema '{schema_name}' does not exist")
                return False
            
            drop_query = f"DROP SCHEMA {schema_name} CASCADE;"
            self.db.execute_query(drop_query, fetch=False)
            logger.info(f"🗑️ Deleted schema: {schema_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting project schema: {e}")
            return False

    def sanitize_name(self, name: str) -> str:
        """Sanitize input for SQL safety."""
        if not name: return "unknown"
        # Basic sanitization
        name = name.strip().lower().replace(' ', '_')
        name = re.sub(r'[^\w]', '', name)
        
        # Ensure it's not empty and starts with letter if possible (relaxed)
        if not name: return "unknown"
        return name
    
    def validate_schema_exists(self, schema_name: str) -> bool:
        """
        Check if a schema exists in the database.
        
        Args:
            schema_name: Schema name to validate
            
        Returns:
            bool: True if schema exists
        """
        try:
            query = """
                SELECT 1 
                FROM information_schema.schemata 
                WHERE schema_name = %s;
            """
            result = self.db.execute_query(query, (schema_name,))
            return len(result) > 0
            
        except Exception as e:
            logger.error(f"Error validating schema '{schema_name}': {e}")
            return False
    
    def get_project_metadata(self, schema_name: str) -> Dict[str, any]:
        """
        Get metadata about a project schema.
        
        Args:
            schema_name: Schema name
            
        Returns:
            Dict: Metadata including table count, row count, creation date
        """
        try:
            # Get table count
            table_query = """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                AND table_name != 'chat_history';
            """
            table_result = self.db.execute_query(table_query, (schema_name,))
            table_count = table_result[0][0] if table_result else 0
            
            # Get total row count across all tables (excluding chat_history)
            tables_list_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
                AND table_name != 'chat_history';
            """
            tables = self.db.execute_query(tables_list_query, (schema_name,))
            
            total_rows = 0
            for table_row in tables:
                table_name = table_row[0]
                try:
                    count_query = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
                    count_result = self.db.execute_query(count_query)
                    total_rows += count_result[0][0] if count_result else 0
                except Exception as e:
                    logger.warning(f"Could not count rows in {schema_name}.{table_name}: {e}")
            
            # Get schema creation time (approximate via oldest table)
            created_at = "Unknown"
            try:
                # Try to get chat_history creation time as proxy for schema creation
                creation_query = f"""
                    SELECT MIN(timestamp) 
                    FROM {schema_name}.chat_history;
                """
                creation_result = self.db.execute_query(creation_query)
                if creation_result and creation_result[0][0]:
                    created_at = creation_result[0][0].strftime("%Y-%m-%d %H:%M")
            except:
                # If chat_history doesn't exist yet, just use "Recently"
                created_at = "Recently"
            
            return {
                'table_count': table_count,
                'total_rows': total_rows,
                'created_at': created_at
            }
            
        except Exception as e:
            logger.error(f"Error getting metadata for schema '{schema_name}': {e}")
            return {
                'table_count': 0,
                'total_rows': 0,
                'created_at': 'Unknown'
            }
    
    def get_schema_tables(self, schema_name: str, exclude_chat_history: bool = True) -> List[str]:
        """
        Get list of tables in a schema.
        
        Args:
            schema_name: Schema name
            exclude_chat_history: Whether to exclude chat_history table
            
        Returns:
            List[str]: Table names
        """
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_type = 'BASE TABLE'
            """
            
            if exclude_chat_history:
                query += " AND table_name != 'chat_history'"
            
            query += " ORDER BY table_name;"
            
            results = self.db.execute_query(query, (schema_name,))
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"Error getting tables for schema '{schema_name}': {e}")
            return []


@st.cache_resource
def get_project_manager() -> ProjectManager:
    """
    Get project manager instance.
    Cached explicitly to reuse SQLAlchemy engine.
    
    Returns:
        ProjectManager: Project manager instance
    """
    return ProjectManager()
