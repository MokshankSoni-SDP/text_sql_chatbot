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
    
    def __init__(self, schema_name: str = None):
        """Initialize schema extractor.
        
        Args:
            schema_name: Schema to extract from (default: from env or 'public')
        """
        self.db = get_db_instance()
        # Accept schema as parameter, fallback to env var, then 'public'
        self.schema_name = schema_name or os.getenv('DB_SCHEMA', 'public')
        self.max_unique_values = int(os.getenv('SCHEMA_MAX_UNIQUE_VALUES', '20'))  # Cardinality threshold
        
        # System/metadata tables to exclude from value enrichment
        # These are internal tables not meant for user queries
        self.excluded_tables = {
            'chat_history',      # Internal chat storage
            'session_data',      # Session management
            'audit_log',         # Audit trails
            'migrations',        # DB migrations
            'schema_version'     # Schema versioning
        }
    
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
    
    def _get_column_values(self, table_name: str, column_name: str) -> List[str]:
        """
        Get distinct values for a text-based column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            List[str]: List of unique values (limited by max_unique_values)
        """
        query = f"""
            SELECT DISTINCT {column_name}
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            ORDER BY {column_name}
            LIMIT %s;
        """
        
        try:
            results = self.db.execute_query(query, (self.max_unique_values + 1,))  # +1 to detect overflow
            values = [str(row[0]) for row in results]
            return values
        except Exception as e:
            logger.warning(f"Could not fetch values for {table_name}.{column_name}: {e}")
            return []
    
    def _check_column_cardinality(self, table_name: str, column_name: str) -> int:
        """
        Check the cardinality (number of unique values) for a column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            int: Number of distinct non-null values
        """
        query = f"""
            SELECT COUNT(DISTINCT {column_name})
            FROM {table_name}
            WHERE {column_name} IS NOT NULL;
        """
        
        try:
            result = self.db.execute_query(query)
            count = result[0][0] if result else 0
            return count
        except Exception as e:
            logger.warning(f"Could not check cardinality for {table_name}.{column_name}: {e}")
            return -1  # Indicate error
    
    def _is_text_column(self, data_type: str) -> bool:
        """
        Check if a column is text-based.
        
        Args:
            data_type: PostgreSQL data type
            
        Returns:
            bool: True if text-based column
        """
        text_types = [
            'character varying', 'varchar', 'character', 'char',
            'text', 'name', 'citext'
        ]
        return any(text_type in data_type.lower() for text_type in text_types)
    
    def extract_enriched_schema(self, table_names: List[str] = None) -> str:
        """
        Extract schema with enriched information (actual column values).
        This helps prevent LLM hallucinations on filter values.
        
        Args:
            table_names: List of table names to extract (optional, extracts all if None)
            
        Returns:
            str: Formatted enriched schema with actual values
        """
        try:
            # Get all tables if not specified
            if table_names is None:
                table_names = self._get_all_tables()
            
            schema_text = self._format_enriched_schema(table_names)
            logger.info(f"Enriched schema extracted for {len(table_names)} table(s)")
            return schema_text
            
        except Exception as e:
            logger.error(f"Error extracting enriched schema: {e}")
            raise
    
    def _format_enriched_schema(self, table_names: List[str]) -> str:
        """
        Format schema with enriched value information.
        
        Args:
            table_names: List of table names to format
            
        Returns:
            str: Formatted enriched schema text
        """
        schema_parts = ["DATABASE SCHEMA (Enriched with actual values):\n"]
        
        for table_name in table_names:
            columns = self._get_table_columns(table_name)
            
            # Check if this is an excluded system table
            is_excluded = table_name.lower() in self.excluded_tables
            
            schema_parts.append(f"\nTable: {table_name}")
            schema_parts.append("=" * (len(table_name) + 7))
            
            if is_excluded:
                # For excluded tables, add a note and show basic schema only
                schema_parts.append("  [System/Internal Table - Basic schema only]")
            
            for col in columns:
                nullable_text = "NULL" if col['nullable'] else "NOT NULL"
                default_text = f" DEFAULT {col['default']}" if col['default'] else ""
                
                col_info = f"  - {col['name']} ({col['type']}) {nullable_text}{default_text}"
                
                # Only enrich values for non-excluded tables
                if not is_excluded and self._is_text_column(col['type']):
                    cardinality = self._check_column_cardinality(table_name, col['name'])
                    
                    if 0 < cardinality <= self.max_unique_values:
                        # Fetch actual values
                        values = self._get_column_values(table_name, col['name'])
                        
                        if len(values) <= self.max_unique_values:
                            # Format values nicely
                            values_str = ", ".join([f"'{v}'" for v in values[:self.max_unique_values]])
                            col_info += f"\n    → Possible Values: [{values_str}]"
                            logger.debug(f"Enriched {table_name}.{col['name']} with {len(values)} values")
                        else:
                            col_info += f"\n    → Note: {cardinality} distinct values (too many to list)"
                    elif cardinality > self.max_unique_values:
                        col_info += f"\n    → Note: High cardinality ({cardinality} distinct values)"
                
                schema_parts.append(col_info)
        
        schema_parts.append("\n" + "=" * 60)
        schema_parts.append("IMPORTANT: When filtering text columns, use ONLY the 'Possible Values' listed above.")
        schema_parts.append("Do NOT guess or assume values that are not explicitly shown.")
        
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


def get_database_schema(schema_name: str = None, table_names: List[str] = None) -> str:
    """
    Convenience function to extract database schema.
    
    Args:
        schema_name: Schema to extract from (default: from env or 'public')
        table_names: List of table names to extract (optional)
        
    Returns:
        str: Formatted schema text
    """
    extractor = SchemaExtractor(schema_name=schema_name)
    return extractor.extract_schema(table_names)


def get_enriched_database_schema(schema_name: str = None, table_names: List[str] = None) -> str:
    """
    Convenience function to extract enriched database schema with actual values.
    This prevents LLM hallucinations by showing real column values.
    
    Args:
        schema_name: Schema to extract from (default: from env or 'public')
        table_names: List of table names to extract (optional)
        
    Returns:
        str: Formatted enriched schema text with actual values
    """
    extractor = SchemaExtractor(schema_name=schema_name)
    return extractor.extract_enriched_schema(table_names)
