import os
import psycopg2
from psycopg2 import pool, OperationalError, DatabaseError
from dotenv import load_dotenv
from typing import Optional, Any
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """
    PostgreSQL database connection manager with connection pooling.
    """
    
    def __init__(self):
        """Initialize connection pool."""
        self.connection_pool: Optional[pool.SimpleConnectionPool] = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Create connection pool using environment variables."""
        try:
            self.connection_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'text_to_sql_chatbot'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            logger.info("Database connection pool created successfully")
        except OperationalError as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            psycopg2.connection: Database connection
            
        Raises:
            OperationalError: If connection cannot be established
        """
        if self.connection_pool:
            try:
                conn = self.connection_pool.getconn()
                logger.debug("Connection retrieved from pool")
                return conn
            except Exception as e:
                logger.error(f"Error getting connection: {e}")
                raise
        else:
            raise OperationalError("Connection pool not initialized")
    
    def return_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: Database connection to return
        """
        if self.connection_pool and conn:
            self.connection_pool.putconn(conn)
            logger.debug("Connection returned to pool")
    
    def close_all_connections(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("All database connections closed")
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = True):
        """
        Execute a SQL query with automatic connection management.
        
        Args:
            query: SQL query to execute
            params: Query parameters (optional)
            fetch: Whether to fetch results (default: True)
            
        Returns:
            List of row tuples if fetch=True, None otherwise
            
        Raises:
            DatabaseError: If query execution fails
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                results = cursor.fetchall()
                return results
            else:
                conn.commit()
                return None
                
        except DatabaseError as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(conn)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all_connections()


# Global database instance
_db_instance: Optional[DatabaseConnection] = None


def get_db_instance() -> DatabaseConnection:
    """
    Get or create the global database instance.
    
    Returns:
        DatabaseConnection: Global database connection instance
    """
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseConnection()
    return _db_instance
