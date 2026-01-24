"""
SQL validation to ensure safe query execution.
Validates that only SELECT queries are executed and prevents dangerous operations.
"""

import re
import logging
from typing import Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLValidator:
    """
    Validates SQL queries for safety.
    """
    
    # Dangerous SQL keywords that should not be allowed
    DANGEROUS_KEYWORDS = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER',
        'CREATE', 'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC',
        'EXECUTE', 'CALL', 'MERGE', 'REPLACE'
    ]
    
    def __init__(self):
        """Initialize SQL validator."""
        pass
    
    def validate(self, sql_query: str) -> Tuple[bool, str]:
        """
        Validate SQL query for safety.
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
                - is_valid: True if query is safe, False otherwise
                - error_message: Error message if invalid, empty string if valid
        """
        if not sql_query or not sql_query.strip():
            return False, "SQL query is empty"
        
        # Normalize query for validation
        normalized_query = sql_query.strip().upper()
        
        # Check if query starts with SELECT
        if not self._starts_with_select(normalized_query):
            logger.warning(f"Query does not start with SELECT: {sql_query[:50]}")
            return False, "Query must start with SELECT. Only SELECT queries are allowed."
        
        # Check for dangerous keywords
        dangerous_found = self._contains_dangerous_keywords(normalized_query)
        if dangerous_found:
            logger.warning(f"Dangerous keyword '{dangerous_found}' found in query")
            return False, f"Dangerous operation detected: {dangerous_found}. Only SELECT queries are allowed."
        
        # Check for multiple statements (prevents SQL injection via chained queries)
        if self._contains_multiple_statements(sql_query):
            logger.warning("Multiple SQL statements detected")
            return False, "Multiple SQL statements are not allowed. Please use a single SELECT query."
        
        logger.info("SQL query validated successfully")
        return True, ""
    
    def _starts_with_select(self, normalized_query: str) -> bool:
        """
        Check if query starts with SELECT (ignoring comments and whitespace).
        
        Args:
            normalized_query: Uppercase version of the query
            
        Returns:
            bool: True if starts with SELECT
        """
        # Remove leading comments and whitespace
        query = self._remove_leading_comments(normalized_query)
        return query.startswith('SELECT')
    
    def _contains_dangerous_keywords(self, normalized_query: str) -> str:
        """
        Check if query contains dangerous keywords.
        
        Args:
            normalized_query: Uppercase version of the query
            
        Returns:
            str: First dangerous keyword found, or empty string if none
        """
        for keyword in self.DANGEROUS_KEYWORDS:
            # Use word boundary regex to avoid false positives
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, normalized_query):
                return keyword
        return ""
    
    def _contains_multiple_statements(self, sql_query: str) -> bool:
        """
        Check if query contains multiple SQL statements.
        
        Args:
            sql_query: Original SQL query
            
        Returns:
            bool: True if multiple statements detected
        """
        # Remove string literals to avoid false positives from semicolons in strings
        query_without_strings = re.sub(r"'[^']*'", "", sql_query)
        
        # Count semicolons (statement separators)
        semicolons = query_without_strings.count(';')
        
        # Allow one trailing semicolon
        if semicolons > 1:
            return True
        
        return False
    
    def _remove_leading_comments(self, query: str) -> str:
        """
        Remove leading SQL comments from query.
        
        Args:
            query: SQL query
            
        Returns:
            str: Query with leading comments removed
        """
        # Remove single-line comments (-- comment)
        lines = query.split('\n')
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('--'):
                return '\n'.join(lines[i:])
        
        return query


def validate_sql(sql_query: str) -> Tuple[bool, str]:
    """
    Convenience function to validate SQL query.
    
    Args:
        sql_query: SQL query to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    validator = SQLValidator()
    return validator.validate(sql_query)
