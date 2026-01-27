"""
Project Manager Module
Manages PostgreSQL schema-based project lifecycle for multi-tenant architecture.
Each user project is isolated in its own schema (proj_{user_id}_{project_name}).
"""

import re
import logging
from typing import List, Dict, Optional
from datetime import datetime
from .db_connection import get_db_instance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProjectManager:
    """
    Manages project/schema lifecycle for multi-tenant Text-to-SQL system.
    """
    
    def __init__(self):
        """Initialize project manager."""
        self.db = get_db_instance()
        self.schema_prefix = "proj_"
    
    def list_user_projects(self, user_id: str) -> List[Dict[str, str]]:
        """
        List all projects for a specific user.
        
        Args:
            user_id: User identifier
            
        Returns:
            List[Dict]: List of project dictionaries with schema_name, display_name, created_at
        """
        try:
            # Sanitize user_id for pattern matching
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
                # Extract project name from schema name
                # Format: proj_{user_id}_{project_name}
                parts = schema_name.split('_', 2)  # Split into max 3 parts
                project_name = parts[2] if len(parts) > 2 else schema_name
                
                # Get metadata
                metadata = self.get_project_metadata(schema_name)
                
                projects.append({
                    'schema_name': schema_name,
                    'display_name': project_name.replace('_', ' ').title(),
                    'project_name': project_name,
                    'created_at': metadata.get('created_at', 'Unknown'),
                    'table_count': metadata.get('table_count', 0),
                    'total_rows': metadata.get('total_rows', 0)
                })
            
            logger.info(f"Found {len(projects)} projects for user '{user_id}'")
            return projects
            
        except Exception as e:
            logger.error(f"Error listing projects for user '{user_id}': {e}")
            return []
    
    def create_project(self, user_id: str, project_name: str) -> str:
        """
        Create a new project schema.
        
        Args:
            user_id: User identifier
            project_name: Project name
            
        Returns:
            str: Created schema name
            
        Raises:
            ValueError: If names are invalid
            Exception: If schema creation fails
        """
        try:
            # Sanitize inputs
            safe_user_id = self.sanitize_name(user_id)
            safe_project_name = self.sanitize_name(project_name)
            
            # Construct schema name
            schema_name = f"{self.schema_prefix}{safe_user_id}_{safe_project_name}"
            
            # Validate length (PostgreSQL limit is 63 characters)
            if len(schema_name) > 63:
                raise ValueError(
                    f"Schema name too long ({len(schema_name)} > 63). "
                    f"Please use shorter user_id or project_name."
                )
            
            # Create schema
            create_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
            self.db.execute_query(create_query, fetch=False)
            
            logger.info(f"✅ Created schema: {schema_name}")
            return schema_name
            
        except ValueError as e:
            logger.error(f"Validation error creating project: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating project schema: {e}")
            raise
    
    def delete_project(self, schema_name: str) -> bool:
        """
        Delete a project schema and all its tables.
        
        Args:
            schema_name: Schema name to delete
            
        Returns:
            bool: True if successful
            
        Raises:
            ValueError: If schema name is invalid or protected
        """
        try:
            # Validate schema name starts with prefix (safety check)
            if not schema_name.startswith(self.schema_prefix):
                raise ValueError(
                    f"Cannot delete schema '{schema_name}'. "
                    f"Only schemas starting with '{self.schema_prefix}' can be deleted."
                )
            
            # Additional safety: prevent deletion of system schemas
            protected_schemas = ['public', 'information_schema', 'pg_catalog', 'pg_toast']
            if schema_name in protected_schemas:
                raise ValueError(f"Cannot delete protected schema: {schema_name}")
            
            # Verify schema exists
            if not self.validate_schema_exists(schema_name):
                logger.warning(f"Schema '{schema_name}' does not exist")
                return False
            
            # Drop schema with CASCADE (removes all tables)
            drop_query = f"DROP SCHEMA {schema_name} CASCADE;"
            self.db.execute_query(drop_query, fetch=False)
            
            logger.info(f"🗑️ Deleted schema: {schema_name}")
            return True
            
        except ValueError as e:
            logger.error(f"Validation error deleting project: {e}")
            raise
        except Exception as e:
            logger.error(f"Error deleting project schema: {e}")
            return False
    
    def sanitize_name(self, name: str) -> str:
        """
        Sanitize user input for use in SQL identifiers.
        Prevents SQL injection and ensures valid PostgreSQL identifiers.
        
        Args:
            name: User-provided name
            
        Returns:
            str: Sanitized name (lowercase, alphanumeric + underscore)
            
        Raises:
            ValueError: If name is invalid
        """
        if not name or not name.strip():
            raise ValueError("Name cannot be empty")
        
        # Remove leading/trailing whitespace
        name = name.strip()
        
        # Replace spaces with underscores
        name = name.replace(' ', '_')
        
        # Remove all non-alphanumeric characters except underscore
        name = re.sub(r'[^\w]', '', name)
        
        # Convert to lowercase
        name = name.lower()
        
        # Ensure starts with a letter (PostgreSQL requirement)
        if not name or not name[0].isalpha():
            raise ValueError(
                "Name must start with a letter. "
                f"Provided: '{name}'"
            )
        
        # Check for valid pattern (alphanumeric + underscore only)
        if not re.match(r'^[a-z][a-z0-9_]*$', name):
            raise ValueError(
                "Name must contain only letters, numbers, and underscores. "
                f"Provided: '{name}'"
            )
        
        # Length check (leave room for prefix)
        if len(name) > 50:
            raise ValueError(f"Name too long (max 50 characters). Provided: {len(name)}")
        
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


def get_project_manager() -> ProjectManager:
    """
    Get project manager instance.
    
    Returns:
        ProjectManager: Project manager instance
    """
    return ProjectManager()
