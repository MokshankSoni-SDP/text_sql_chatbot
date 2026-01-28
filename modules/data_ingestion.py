"""
Data Ingestion Module
Handles file uploads (CSV, Excel, JSON) and creates tables in user schemas.
Supports dynamic type inference and column name sanitization.
"""

import re
import logging
import pandas as pd
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime, Text
from sqlalchemy import types as sqltypes
import os
from dotenv import load_dotenv
from .db_connection import get_db_instance
from .embedding_service import get_embedding_service

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataIngestor:
    """
 file uploads into PostgreSQL schemas."""
    
    def __init__(self):
        """Initialize data ingestor."""
        self.db = get_db_instance()
        
        # Build SQLAlchemy connection string
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'text_to_sql_chatbot')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.engine = create_engine(connection_string)
    
    def ingest_csv(self, file_path: str, schema_name: str, table_name: Optional[str] = None) -> Tuple[bool, str]:
        """
        Ingest CSV file into a schema.
        
        Args:
            file_path: Path to CSV file
            schema_name: Target schema name
            table_name: Optional table name (defaults to sanitized filename)
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Read CSV
            df = pd.read_csv(file_path)
            logger.info(f"Read CSV file: {file_path}, Shape: {df.shape}")
            
            # Determine table name
            if table_name is None:
                file_name = Path(file_path).stem
                table_name = self.sanitize_table_name(file_name)
            else:
                table_name = self.sanitize_table_name(table_name)
            
            # Sanitize column names
            df.columns = [self.sanitize_column_name(col) for col in df.columns]
            
            # Create table from dataframe
            success, msg = self.create_table_from_dataframe(df, table_name, schema_name)
            
            if success:
                logger.info(f"✅ CSV ingested: {table_name} ({len(df)} rows)")
                return True, f"Successfully created table '{table_name}' with {len(df)} rows"
            else:
                return False, msg
            
        except Exception as e:
            error_msg = f"Error ingesting CSV: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def ingest_excel(self, file_path: str, schema_name: str) -> Tuple[bool, str]:
        """
        Ingest Excel file into a schema.
        Creates one table per sheet.
        
        Args:
            file_path: Path to Excel file
            schema_name: Target schema name
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            
            logger.info(f"Read Excel file: {file_path}, Sheets: {sheet_names}")
            
            created_tables = []
            errors = []
            
            for sheet_name in sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Sanitize sheet name for table name
                table_name = self.sanitize_table_name(sheet_name)
                
                # Sanitize column names
                df.columns = [self.sanitize_column_name(col) for col in df.columns]
                
                # Create table
                success, msg = self.create_table_from_dataframe(df, table_name, schema_name)
                
                if success:
                    created_tables.append(f"{table_name} ({len(df)} rows)")
                    logger.info(f"✅ Created table from sheet '{sheet_name}': {table_name}")
                else:
                    errors.append(f"{sheet_name}: {msg}")
                    logger.error(f"❌ Failed to create table from sheet '{sheet_name}': {msg}")
            
            if created_tables:
                success_msg = f"Created {len(created_tables)} table(s): " + ", ".join(created_tables)
                if errors:
                    success_msg += f"\nErrors: " + "; ".join(errors)
                return True, success_msg
            else:
                return False, "Failed to create any tables. Errors: " + "; ".join(errors)
            
        except Exception as e:
            error_msg = f"Error ingesting Excel: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def ingest_json(self, file_path: str, schema_name: str, table_name: Optional[str] = None) -> Tuple[bool, str]:
        """
        Ingest JSON file into a schema.
        Automatically flattens nested structures.
        
        Args:
            file_path: Path to JSON file
            schema_name: Target schema name
            table_name: Optional table name (defaults to sanitized filename)
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Try to read JSON (handles both array and object formats)
            try:
                df = pd.read_json(file_path)
            except ValueError:
                # If normal read fails, try with records orientation
                df = pd.read_json(file_path, lines=True)
            
            logger.info(f"Read JSON file: {file_path}, Shape: {df.shape}")
            
            # Flatten nested structures if present
            if any(df.dtypes == 'object'):
                try:
                    df = pd.json_normalize(df.to_dict('records'))
                    logger.info("Flattened nested JSON structures")
                except:
                    logger.warning("Could not flatten JSON, using as-is")
            
            # Determine table name
            if table_name is None:
                file_name = Path(file_path).stem
                table_name = self.sanitize_table_name(file_name)
            else:
                table_name = self.sanitize_table_name(table_name)
            
            # Sanitize column names
            df.columns = [self.sanitize_column_name(col) for col in df.columns]
            
            # Create table from dataframe
            success, msg = self.create_table_from_dataframe(df, table_name, schema_name)
            
            if success:
                logger.info(f"✅ JSON ingested: {table_name} ({len(df)} rows)")
                return True, f"Successfully created table '{table_name}' with {len(df)} rows"
            else:
                return False, msg
            
        except Exception as e:
            error_msg = f"Error ingesting JSON: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def create_table_from_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str, 
        schema_name: str
    ) -> Tuple[bool, str]:
        """
        Create a table from a pandas DataFrame using SQLAlchemy.
        NOW WITH EMBEDDING GENERATION for hybrid search.
        
        Args:
            df: Pandas DataFrame
            table_name: Name of table to create
            schema_name: Schema to create table in
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Detect text-heavy columns for embeddings
            text_columns = self._detect_text_columns(df)
            
            logger.info(f"Detected {len(text_columns)} text columns for embeddings: {text_columns}")
            
            # Generate embeddings if text columns exist
            if text_columns:
                logger.info("Generating embeddings for semantic search...")
                embedding_service = get_embedding_service()
                
                # Build combined context for each row
                contexts = []
                for idx, row in df.iterrows():
                    context = self._build_embedding_context(row, text_columns)
                    contexts.append(context)
                
                # Generate batch embeddings
                embeddings = embedding_service.generate_batch_embeddings(contexts, batch_size=32)
                
                # Add embedding column to dataframe
                df['embedding'] = embeddings
                
                logger.info(f"✅ Generated {len(embeddings)} embeddings ({embedding_service.get_embedding_dim()}-dim)")
            else:
                logger.info("No suitable text columns found, skipping embedding generation")
            
            # Use pandas to_sql with SQLAlchemy engine
            df_to_save = df.copy()
            
            # Convert embedding lists to strings for PostgreSQL vector type
            if 'embedding' in df_to_save.columns:
                df_to_save['embedding'] = df_to_save['embedding'].apply(
                    lambda x: '[' + ','.join(map(str, x)) + ']' if isinstance(x, list) else None
                )
            
            # Save to database
            df_to_save.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema_name,
                if_exists='replace',  # Replace if exists
                index=False,  # Don't create index column
                method='multi',  # Use multi-row INSERT for better performance
                chunksize=1000,
                dtype={'embedding': Text}  # Store as text first
            )
            
            # Convert embedding column to vector type if it exists
            if 'embedding' in df.columns:
                self._convert_embedding_to_vector(schema_name, table_name)
            
            logger.info(f"✅ Created table {schema_name}.{table_name} with {len(df)} rows")
            
            if text_columns:
                return True, f"Table created successfully with {len(embeddings)} embeddings for semantic search"
            else:
                return True, f"Table created successfully"
            
        except Exception as e:
            error_msg = f"Error creating table: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def _detect_text_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect text-heavy columns suitable for embedding generation.
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            List[str]: List of text column names
        """
        text_columns = []
        
        for col in df.columns:
            # Check if column is object/string type
            if df[col].dtype == 'object':
                # Calculate average text length
                avg_length = df[col].astype(str).str.len().mean()
                
                # Include if average length > 10 characters
                if avg_length > 10:
                    text_columns.append(col)
                    logger.debug(f"Column '{col}' selected (avg length: {avg_length:.1f})")
        
        return text_columns
    
    def _build_embedding_context(self, row: pd.Series, text_columns: List[str]) -> str:
        """
        Build combined text context from multiple columns for embedding.
        
        Args:
            row: DataFrame row
            text_columns: List of text column names
            
        Returns:
            str: Combined context string
        """
        parts = []
        
        for col in text_columns:
            value = row.get(col)
            
            if pd.notna(value) and str(value).strip():
                # Format: "Column Name: value"
                formatted_col = col.replace('_', ' ').title()
                parts.append(f"{formatted_col}: {value}")
        
        # Combine with periods
        context = ". ".join(parts)
        
        return context if context else "No description available"
    
    def _convert_embedding_to_vector(self, schema_name: str, table_name: str):
        """
        Convert embedding column from text to vector type.
        
        Args:
            schema_name: Schema name
            table_name: Table name
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Alter column to vector type
            alter_query = f"""
                ALTER TABLE {schema_name}.{table_name}
                ALTER COLUMN embedding TYPE vector(384)
                USING embedding::vector;
            """
            
            cursor.execute(alter_query)
            conn.commit()
            cursor.close()
            
            logger.info(f"✅ Converted embedding column to vector(384) type")
            
            # Create index for faster similarity search
            try:
                index_query = f"""
                    CREATE INDEX IF NOT EXISTS {table_name}_embedding_idx
                    ON {schema_name}.{table_name}
                    USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = 100);
                """
                
                cursor = conn.cursor()
                cursor.execute(index_query)
                conn.commit()
                cursor.close()
                
                logger.info(f"✅ Created vector index for faster search")
            except Exception as idx_error:
                logger.warning(f"Could not create vector index: {idx_error}")
            
        except Exception as e:
            logger.error(f"Error converting to vector type: {e}")
            # Don't fail the entire ingestion if vector conversion fails
    
    def sanitize_column_name(self, col: str) -> str:
        """
        Sanitize column name for SQL compatibility.
        
        Args:
            col: Original column name
            
        Returns:
            str: Sanitized column name
        """
        if not col or not str(col).strip():
            return "unnamed_column"
        
        col = str(col).strip()
        
        # Replace spaces and special characters with underscores
        col = re.sub(r'[^\w]', '_', col)
        
        # Remove consecutive underscores
        col = re.sub(r'_+', '_', col)
        
        # Convert to lowercase
        col = col.lower()
        
        # Ensure doesn't start with number
        if col and col[0].isdigit():
            col = f"col_{col}"
        
        # Ensure not empty after sanitization
        if not col:
            col = "unnamed_column"
        
        # Truncate if too long (PostgreSQL limit is 63 chars)
        if len(col) > 63:
            col = col[:63]
        
        return col
    
    def sanitize_table_name(self, name: str) -> str:
        """
        Sanitize table name for SQL compatibility.
        
        Args:
            name: Original table name
            
        Returns:
            str: Sanitized table name
        """
        if not name or not str(name).strip():
            return "data_table"
        
        name = str(name).strip()
        
        # Replace spaces and special characters with underscores
        name = re.sub(r'[^\w]', '_', name)
        
        # Remove consecutive underscores
        name = re.sub(r'_+', '_', name)
        
        # Convert to lowercase
        name = name.lower()
        
        # Ensure starts with letter
        if name and not name[0].isalpha():
            name = f"table_{name}"
        
        # Ensure not empty
        if not name:
            name = "data_table"
        
        # Truncate if too long
        if len(name) > 63:
            name = name[:63]
        
        return name
    
    def get_dataframe_preview(self, df: pd.DataFrame, n_rows: int = 5) -> Dict:
        """
        Get a preview of the DataFrame for user confirmation.
        
        Args:
            df: Pandas DataFrame
            n_rows: Number of rows to preview
            
        Returns:
            Dict: Preview information
        """
        return {
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'head': df.head(n_rows).to_dict('records'),
            'null_counts': df.isnull().sum().to_dict()
        }


def get_data_ingestor() -> DataIngestor:
    """
    Get data ingestor instance.
    
    Returns:
        DataIngestor: Data ingestor instance
    """
    return DataIngestor()
