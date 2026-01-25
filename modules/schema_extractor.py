import os
from typing import Dict, List
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


class SchemaExtractor:
    """
    Extracts and formats database schema information.
    """
    
    def __init__(self):
        """Initialize schema extractor."""
        self.db = get_db_instance()
        self.schema_name = os.getenv('DB_SCHEMA', 'public')
    
    def extract_schema(self, table_names: List[str] = None) -> str:
        """
        Extract schema for specified tables or all tables in the schema.
        
        Args:
            table_names: List of table names to extract (optional, extracts all if None)
            
        Returns:
            str: Formatted schema as human-readable text
        """
        try:
            # Get all tables if not specified
            if table_names is None:
                table_names = self._get_all_tables()
            
            schema_text = self._format_schema(table_names)
            logger.info(f"Schema extracted for {len(table_names)} table(s)")
            return schema_text
            
        except Exception as e:
            logger.error(f"Error extracting schema: {e}")
            raise
    
    def _get_all_tables(self) -> List[str]:
        """
        Get all table names in the schema.
        
        Returns:
            List[str]: List of table names
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        
        try:
            results = self.db.execute_query(query, (self.schema_name,))
            table_names = [row[0] for row in results]
            logger.debug(f"Found {len(table_names)} tables in schema '{self.schema_name}'")
            return table_names
        except Exception as e:
            logger.error(f"Error fetching table names: {e}")
            raise
    
    def _get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get column information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List[Dict]: List of column dictionaries with name, type, and nullable info
        """
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s 
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        
        try:
            results = self.db.execute_query(query, (self.schema_name, table_name))
            columns = [
                {
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'default': row[3]
                }
                for row in results
            ]
            return columns
        except Exception as e:
            logger.error(f"Error fetching columns for table '{table_name}': {e}")
            raise
    
    def _format_schema(self, table_names: List[str]) -> str:
        """
        Format schema information into human-readable text.
        
        Args:
            table_names: List of table names to format
            
        Returns:
            str: Formatted schema text
        """
        schema_parts = ["DATABASE SCHEMA:\n"]
        
        for table_name in table_names:
            columns = self._get_table_columns(table_name)
            
            schema_parts.append(f"\nTable: {table_name}")
            schema_parts.append("-" * (len(table_name) + 7))
            
            for col in columns:
                nullable_text = "NULL" if col['nullable'] else "NOT NULL"
                default_text = f" DEFAULT {col['default']}" if col['default'] else ""
                schema_parts.append(
                    f"  - {col['name']} ({col['type']}) {nullable_text}{default_text}"
                )
        
        return "\n".join(schema_parts)
    
    def get_schema_summary(self, table_names: List[str] = None) -> Dict[str, int]:
        """
        Get a summary of the schema.
        
        Args:
            table_names: List of table names (optional)
            
        Returns:
            Dict: Summary statistics
        """
        if table_names is None:
            table_names = self._get_all_tables()
        
        total_columns = 0
        for table_name in table_names:
            columns = self._get_table_columns(table_name)
            total_columns += len(columns)
        
        return {
            'total_tables': len(table_names),
            'total_columns': total_columns
        }


def get_database_schema(table_names: List[str] = None) -> str:
    """
    Convenience function to extract database schema.
    
    Args:
        table_names: List of table names to extract (optional)
        
    Returns:
        str: Formatted schema text
    """
    extractor = SchemaExtractor()
    return extractor.extract_schema(table_names)
