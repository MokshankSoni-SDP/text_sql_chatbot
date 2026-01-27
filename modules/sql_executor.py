"""
SQL execution handler with error management.
Executes validated SQL queries and manages results.
"""

import logging
from typing import Tuple, List, Optional
import pandas as pd
from psycopg2 import DatabaseError
from .db_connection import get_db_instance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLExecutor:
    """
    Executes SQL queries and manages results.
    """
    
    def __init__(self):
        """Initialize SQL executor."""
        self.db = get_db_instance()
    
    def execute(self, sql_query: str, schema_name: str = 'public') -> Tuple[bool, Optional[List[tuple]], Optional[List[str]], str]:
        """
        Execute a SQL query and return results.
        
        Args:
            sql_query: Validated SQL query to execute
            schema_name: Schema to execute query in (default: 'public')
            
        Returns:
            Tuple containing:
                - success (bool): True if execution succeeded
                - results (List[tuple]): Query results as list of tuples, None if error
                - column_names (List[str]): Column names, None if error
                - error_message (str): Error message if failed, empty if successful
        """
        try:
            # Get connection and execute
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Set search path for schema context
            search_path_query = f"SET search_path TO {schema_name}, public;"
            cursor.execute(search_path_query)
            
            logger.info(f"Executing query in schema '{schema_name}': {sql_query[:100]}...")
            cursor.execute(sql_query)
            
            # Fetch results
            results = cursor.fetchall()
            
            # Get column names from cursor description
            column_names = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Close cursor and return connection
            cursor.close()
            self.db.return_connection(conn)
            
            logger.info(f"Query executed successfully. Rows returned: {len(results)}")
            return True, results, column_names, ""
            
        except DatabaseError as e:
            logger.error(f"Database error during query execution: {e}")
            return False, None, None, f"Database error: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}")
            return False, None, None, f"Execution error: {str(e)}"
    
    def execute_to_dataframe(self, sql_query: str, schema_name: str = 'public') -> Tuple[bool, Optional[pd.DataFrame], str]:
        """
        Execute a SQL query and return results as a pandas DataFrame.
        
        Args:
            sql_query: Validated SQL query to execute
            schema_name: Schema to execute query in (default: 'public')
            
        Returns:
            Tuple containing:
                - success (bool): True if execution succeeded
                - dataframe (pd.DataFrame): Results as DataFrame, None if error
                - error_message (str): Error message if failed, empty if successful
        """
        success, results, column_names, error_msg = self.execute(sql_query, schema_name=schema_name)
        
        if not success:
            return False, None, error_msg
        
        try:
            # Convert to DataFrame
            if results and column_names:
                df = pd.DataFrame(results, columns=column_names)
            else:
                # Empty result
                df = pd.DataFrame()
            
            return True, df, ""
        except Exception as e:
            logger.error(f"Error converting results to DataFrame: {e}")
            return False, None, f"Error formatting results: {str(e)}"
    
    def get_result_summary(self, results: List[tuple], column_names: List[str]) -> dict:
        """
        Get a summary of query results.
        
        Args:
            results: Query results as list of tuples
            column_names: Column names
            
        Returns:
            dict: Summary statistics
        """
        summary = {
            'row_count': len(results),
            'column_count': len(column_names),
            'column_names': column_names,
            'has_data': len(results) > 0
        }
        
        return summary


def execute_sql(sql_query: str, schema_name: str = 'public') -> Tuple[bool, Optional[List[tuple]], Optional[List[str]], str]:
    """
    Convenience function to execute SQL query.
    
    Args:
        sql_query: SQL query to execute
        schema_name: Schema to execute query in (default: 'public')
        
    Returns:
        Tuple[bool, List[tuple], List[str], str]: (success, results, column_names, error_message)
    """
    executor = SQLExecutor()
    return executor.execute(sql_query, schema_name=schema_name)
