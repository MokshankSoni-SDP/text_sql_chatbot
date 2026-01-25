import os
from typing import Optional, List, Dict
from dotenv import load_dotenv
import logging
from pathlib import Path
from groq import Groq

# Load environment variables from project root
current_dir = Path(__file__).resolve().parent.parent
env_path = current_dir / '.env'
load_dotenv(dotenv_path=env_path)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GroqLLMClient:
    """
    Groq LLaMA client for text-to-SQL conversion and answer generation.
    """
    
    def __init__(self):
        """Initialize Groq client."""
        self.api_key = os.getenv('GROQ_API_KEY')
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
        
        self.model = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')
        self.client = Groq(api_key=self.api_key)
        logger.info(f"Groq client initialized with model: {self.model}")
    
    def summarize_text(self, text: str) -> str:
        """
        Intelligently summarize text using LLM to preserve key information.
        
        Args:
            text: Text to summarize
            
        Returns:
            str: Summarized text (or original if already short)
        """
        # If text is already short, return as is
        if len(text) <= 300:
            return text
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a text summarizer. Summarize the given text in 1-2 concise sentences. "
                            "Keep specific numbers, filters, data findings, and key facts. "
                            "Remove polite filler text, greetings, and unnecessary details. "
                            "Be direct and factual."
                        )
                    },
                    {
                        "role": "user",
                        "content": f"Summarize this: {text}"
                    }
                ],
                temperature=0.1,
                max_tokens=100
            )
            
            summary = response.choices[0].message.content.strip()
            logger.info(f"Summarized text from {len(text)} to {len(summary)} characters")
            return summary
            
        except Exception as e:
            logger.error(f"Error summarizing text: {e}")
            # Fallback to original text if summarization fails
            return text
    
    
    def text_to_sql(
        self,
        user_question: str,
        schema: str,
        chat_history: str = ""
    ) -> str:
        """
        Convert natural language question to SQL query.
        
        Args:
            user_question: User's question in natural language
            schema: Database schema information
            chat_history: Previous conversation context (optional)
            
        Returns:
            str: Generated SQL query
            
        Raises:
            Exception: If API call fails
        """
        prompt = self._build_sql_prompt(user_question, schema, chat_history)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a SQL expert. Generate ONLY valid PostgreSQL SELECT queries. "
                            "Rules:\n"
                            "1. Return ONLY the SQL query, no explanations or markdown\n"
                            "2. Use SELECT statements ONLY\n"
                            "3. NO INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, or TRUNCATE\n"
                            "4. Base your query strictly on the provided schema\n"
                            "5. Make NO assumptions about data or columns not in the schema\n"
                            "6. CRITICALLY IMPORTANT: When filtering text columns, use ONLY the 'Possible Values' shown in the schema\n"
                            "7. If the question cannot be answered with the given schema, return: SELECT 'Unable to generate query - insufficient schema information' AS error;"
                        )
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            sql_query = self._clean_sql_output(sql_query)
            
            logger.info(f"Generated SQL query: {sql_query[:100]}...")
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            raise
    
    def retry_query_on_empty_results(
        self,
        failed_sql: str,
        user_question: str,
        schema: str,
        chat_history: str = ""
    ) -> str:
        """
        Retry SQL generation with corrective feedback when initial query returns 0 results.
        This helps fix hallucinated filter values.
        
        Args:
            failed_sql: The SQL query that returned empty results
            user_question: Original user question
            schema: Database schema with possible values
            chat_history: Previous conversation context
            
        Returns:
            str: Corrected SQL query
        """
        corrective_prompt = f"""The previous query returned 0 results. This likely means you used an invalid filter value.

FAILED QUERY:
{failed_sql}

ORIGINAL QUESTION:
{user_question}

{schema}

{chat_history if chat_history else ''}

CRITICAL INSTRUCTIONS FOR RETRY:
1. Review the 'Possible Values' list for each text column in the schema above
2. Check if you used a filter value that is NOT in the 'Possible Values' list
3. Use ONLY values explicitly listed in the schema - do NOT guess or assume
4. If the user's question mentions a value not in the schema, find the closest matching value from the 'Possible Values'
5. Consider using ILIKE '%pattern%' for partial matching if exact values don't exist
6. Generate a CORRECTED query that uses only valid values from the schema

Generate the corrected SQL query now:"""

        try:
            logger.info("🔄 Retrying query with corrective feedback...")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a SQL expert fixing a failed query. "
                            "The previous query returned 0 results due to invalid filter values. "
                            "Use ONLY the 'Possible Values' explicitly listed in the schema. "
                            "Return ONLY the corrected SQL query, no explanations."
                        )
                    },
                    {
                        "role": "user",
                        "content": corrective_prompt
                    }
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            corrected_sql = response.choices[0].message.content.strip()
            corrected_sql = self._clean_sql_output(corrected_sql)
            
            logger.info(f"✅ Generated corrected query: {corrected_sql[:100]}...")
            return corrected_sql
            
        except Exception as e:
            logger.error(f"Error generating corrected SQL: {e}")
            raise

    
    def result_to_english(
        self,
        user_question: str,
        sql_query: str,
        sql_result: List[tuple],
        column_names: List[str] = None
    ) -> str:
        """
        Convert SQL results to natural language answer.
        
        Args:
            user_question: Original user question
            sql_query: SQL query that was executed
            sql_result: Query results as list of tuples
            column_names: Column names from the result (optional)
            
        Returns:
            str: Natural language answer
            
        Raises:
            Exception: If API call fails
        """
        prompt = self._build_answer_prompt(
            user_question,
            sql_query,
            sql_result,
            column_names
        )
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a helpful assistant that explains database query results. "
                            "Provide concise, natural language answers based on the data. "
                            "Do NOT include the SQL query in your response. "
                            "If there are no results, say so clearly."
                        )
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,
                max_tokens=300
            )
            
            answer = response.choices[0].message.content.strip()
            logger.info("Generated natural language answer")
            return answer
            
        except Exception as e:
            logger.error(f"Error generating answer: {e}")
            raise
    
    def _build_sql_prompt(
        self,
        user_question: str,
        schema: str,
        chat_history: str
    ) -> str:
        """Build prompt for SQL generation."""
        parts = []
        
        if chat_history:
            parts.append(chat_history)
            parts.append("")
        
        parts.append(schema)
        parts.append("")
        parts.append(f"USER QUESTION: {user_question}")
        parts.append("")
        parts.append("Generate a SQL query to answer this question.")
        
        return "\n".join(parts)
    
    def _build_answer_prompt(
        self,
        user_question: str,
        sql_query: str,
        sql_result: List[tuple],
        column_names: List[str]
    ) -> str:
        """Build prompt for answer generation."""
        parts = [
            f"User asked: {user_question}",
            "",
            f"SQL Query executed: {sql_query}",
            "",
            "Query Results:"
        ]
        
        if not sql_result:
            parts.append("No results found.")
        else:
            # Format results
            if column_names:
                parts.append(f"Columns: {', '.join(column_names)}")
            
            parts.append(f"Number of rows: {len(sql_result)}")
            parts.append("")
            
            # Show first few rows
            max_rows = min(10, len(sql_result))
            for i, row in enumerate(sql_result[:max_rows]):
                parts.append(f"Row {i+1}: {row}")
            
            if len(sql_result) > max_rows:
                parts.append(f"... and {len(sql_result) - max_rows} more rows")
        
        parts.append("")
        parts.append("Provide a clear, concise answer to the user's question based on these results.")
        
        return "\n".join(parts)
    
    def _clean_sql_output(self, sql: str) -> str:
        """Remove markdown code blocks and extra whitespace from SQL output."""
        # Remove markdown code blocks
        if sql.startswith("```"):
            lines = sql.split("\n")
            # Remove first and last lines if they are markdown markers
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            sql = "\n".join(lines)
        
        return sql.strip()


def get_llm_client() -> GroqLLMClient:
    """
    Get Groq LLM client instance.
    
    Returns:
        GroqLLMClient: LLM client instance
    """
    return GroqLLMClient()
