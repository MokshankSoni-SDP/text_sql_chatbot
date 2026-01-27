import os
import psycopg2
from psycopg2 import pool, OperationalError, DatabaseError
from dotenv import load_dotenv
from typing import Optional, Any
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from project root
current_dir = Path(__file__).resolve().parent.parent
env_path = current_dir / '.env'
load_dotenv(dotenv_path=env_path)


class DatabaseConnection:
    """
    Robust PostgreSQL connection manager with connection pooling.
    Automatically handles SSL for Cloud DBs (Aiven/Neon/AWS) and plain auth for local DB.
    """
    
    def __init__(self):
        """Initialize connection pool."""
        self.connection_pool: Optional[pool.SimpleConnectionPool] = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """
        Create connection pool with intelligent SSL handling.
        Detects cloud providers and configures SSL automatically.
        """
        try:
            # 1. Load credentials from environment
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'text_to_sql_chatbot')
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')

            # 2. Base connection arguments
            conn_args = {
                "host": db_host,
                "port": db_port,
                "database": db_name,
                "user": db_user,
                "password": db_password,
                "minconn": 1,
                "maxconn": 10
            }

            # 3. Smart SSL configuration
            # Detect if connecting to a cloud database provider
            is_cloud_db = any(domain in db_host for domain in ['aivencloud.com', 'neon.tech', 'aws.com'])
            
            if is_cloud_db:
                logger.info(f"☁️  Cloud database detected ({db_host}). Configuring SSL...")
                
                # Check for SSL certificate file
                cert_path = current_dir / 'ca.pem'
                
                if 'aivencloud.com' in db_host:
                    if cert_path.exists():
                        conn_args["sslmode"] = "verify-ca"
                        conn_args["sslrootcert"] = str(cert_path)
                        logger.info(f"🔐 Aiven SSL: Using certificate at {cert_path}")
                    else:
                        # Fallback if certificate is missing
                        conn_args["sslmode"] = "require"
                        logger.warning(f"⚠️  Aiven SSL: 'ca.pem' not found! Defaulting to sslmode=require")
                
                elif 'neon.tech' in db_host:
                    # Neon typically just needs require mode
                    conn_args["sslmode"] = "require"
                    logger.info("🔐 Neon SSL: Enabled (sslmode=require)")
                
                elif 'aws.com' in db_host:
                    conn_args["sslmode"] = "require"
                    logger.info("🔐 AWS RDS SSL: Enabled (sslmode=require)")
            
            else:
                # Local database or private server
                logger.info(f"🏠 Local database detected. SSL disabled.")
                conn_args["sslmode"] = "prefer"  # Works for both local and secure

            # 4. Create the connection pool
            self.connection_pool = pool.SimpleConnectionPool(**conn_args)
            logger.info("✅ Database connection pool created successfully")

        except OperationalError as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error initializing pool: {e}")
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
    
    def set_search_path(self, schema_name: str):
        """
        Set the active schema search path for subsequent queries.
        This allows queries to reference tables without schema prefix.
        
        Args:
            schema_name: Schema name to set as active
            
        Raises:
            ValueError: If schema name is invalid
            DatabaseError: If setting search path fails
        """
        # Validate schema name format (security check)
        import re
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', schema_name):
            raise ValueError(f"Invalid schema name: {schema_name}")
        
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Set search path (schema_name first, then public for fallback)
            query = f"SET search_path TO {schema_name}, public;"
            cursor.execute(query)
            conn.commit()
            
            logger.info(f"Set search path to: {schema_name}")
            
        except DatabaseError as e:
            if conn:
                conn.rollback()
            logger.error(f"Error setting search path: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(conn)
    
    def execute_with_schema(
        self, 
        query: str, 
        schema_name: str, 
        params: tuple = None, 
        fetch: bool = True
    ):
        """
        Execute a query with a specific schema context.
        Sets search_path before executing the query.
        
        Args:
            query: SQL query to execute
            schema_name: Schema to set as active
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
            
            # Validate schema name
            import re
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', schema_name):
                raise ValueError(f"Invalid schema name: {schema_name}")
            
            # Set search path
            search_path_query = f"SET search_path TO {schema_name}, public;"
            cursor.execute(search_path_query)
            
            # Execute main query
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                results = cursor.fetchall()
                conn.commit()
                return results
            else:
                conn.commit()
                return None
                
        except DatabaseError as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error in execute_with_schema: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(conn)


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
