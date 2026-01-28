"""
Hybrid Search Module
Executes SQL + Vector similarity searches using pgvector.
"""

import logging
from typing import List, Dict, Tuple, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from .db_connection import get_db_instance

logger = logging.getLogger(__name__)


class HybridSearchEngine:
    """Hybrid search combining SQL filtering with vector similarity."""
    
    def __init__(self):
        """Initialize hybrid search engine."""
        self.db = get_db_instance()
    
    def execute_hybrid_search(
        self,
        schema_name: str,
        table_name: str,
        sql_filters: str,
        query_embedding: List[float],
        limit: int = 10,
        similarity_threshold: float = 0.3
    ) -> Tuple[bool, List[Dict], List[str], Optional[str]]:
        """
        Execute hybrid search: SQL filters + vector similarity.
        
        Args:
            schema_name: Database schema name
            table_name: Table name
            sql_filters: SQL WHERE clause (can be "TRUE" for no filters)
            query_embedding: Query embedding vector (384-dim)
            limit: Maximum number of results
            similarity_threshold: Minimum similarity score (0-1)
            
        Returns:
            Tuple[bool, List[Dict], List[str], Optional[str]]:
                (success, results, column_names, error_message)
        """
        try:
            # Build hybrid query
            query = f"""
                SELECT *,
                       1 - (embedding <=> %s::vector) AS similarity_score
                FROM {schema_name}.{table_name}
                WHERE {sql_filters}
                ORDER BY similarity_score DESC
                LIMIT %s;
            """
            
            # Convert embedding to PostgreSQL vector format
            vector_str = '[' + ','.join(map(str, query_embedding)) + ']'
            
            # Execute query
            conn = self.db.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            logger.info(f"Executing hybrid search on {schema_name}.{table_name}")
            logger.debug(f"SQL filters: {sql_filters}")
            
            cursor.execute(query, (vector_str, limit))
            
            rows = cursor.fetchall()
            
            # Get column names
            if rows:
                column_names = list(rows[0].keys())
            else:
                # Get column names even if no results
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                """, (schema_name, table_name))
                column_names = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            
            # Filter by similarity threshold
            filtered_results = []
            for row in rows:
                row_dict = dict(row)
                similarity = row_dict.get('similarity_score', 0)
                
                if similarity >= similarity_threshold:
                    filtered_results.append(row_dict)
            
            logger.info(f"✅ Hybrid search returned {len(filtered_results)} results (threshold: {similarity_threshold})")
            
            return True, filtered_results, column_names, None
        
        except Exception as e:
            error_msg = f"Hybrid search error: {str(e)}"
            logger.error(error_msg)
            return False, [], [], error_msg
    
    def execute_sql_only_search(
        self,
        schema_name: str,
        table_name: str,
        sql_filters: str,
        limit: int = 10
    ) -> Tuple[bool, List[Dict], List[str], Optional[str]]:
        """
        Execute SQL-only search (fallback when no semantic query).
        
        Args:
            schema_name: Database schema name
            table_name: Table name
            sql_filters: SQL WHERE clause
            limit: Maximum number of results
            
        Returns:
            Tuple[bool, List[Dict], List[str], Optional[str]]:
                (success, results, column_names, error_message)
        """
        try:
            query = f"""
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE {sql_filters}
                LIMIT %s;
            """
            
            conn = self.db.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            logger.info(f"Executing SQL-only search on {schema_name}.{table_name}")
            
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            
            # Get column names
            if rows:
                column_names = list(rows[0].keys())
            else:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                """, (schema_name, table_name))
                column_names = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            
            results = [dict(row) for row in rows]
            
            logger.info(f"✅ SQL search returned {len(results)} results")
            
            return True, results, column_names, None
        
        except Exception as e:
            error_msg = f"SQL search error: {str(e)}"
            logger.error(error_msg)
            return False, [], [], error_msg
    
    def check_vector_column_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Check if embedding column exists in table.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            bool: True if embedding column exists
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                  AND lower(table_name) = lower(%s) 
                  AND column_name = 'embedding';
            """, (schema_name, table_name))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result is not None
        
        except Exception as e:
            logger.error(f"Error checking vector column: {e}")
            return False


def get_hybrid_search_engine() -> HybridSearchEngine:
    """
    Get hybrid search engine instance.
    
    Returns:
        HybridSearchEngine: Hybrid search engine
    """
    return HybridSearchEngine()
