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
        if len(text) <= 150:
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
    
    def classify_intent(self, user_query: str, chat_history: str = "") -> str:
        """
        Classify user intent to determine if query needs database access.
        
        Args:
            user_query: User's input message
            chat_history: Previous conversation context
            
        Returns:
            str: Either "NEEDS_DATABASE" or "GENERAL_CHAT"
        """
        try:
            prompt = f"""Previous conversation:
{chat_history if chat_history else "No previous conversation"}

Current user message: "{user_query}"

Task: Classify this message into one of two categories.

CRITICAL RULES:
1. If the message asks to SHOW, FIND, LIST, COUNT, GET, DISPLAY, or RETRIEVE any data → NEEDS_DATABASE
2. If the message mentions PRODUCTS, SALES, USERS, ORDERS, PRICES, or any data entities → NEEDS_DATABASE
3. If the message contains words like "best", "top", "all", "average", "total" about data → NEEDS_DATABASE
4. ONLY classify as GENERAL_CHAT if it's clearly: greetings, thanks, or meta-questions about SQL/concepts

Examples of NEEDS_DATABASE:
- "Show me all products"
- "Show me your best shoes" ← NEEDS_DATABASE (wants shoe data)
- "What are the top sales?"
- "Display Nike products"
- "Count total orders"
- "List the cheapest items"
- "Find red shoes"
- "Get all users"

Examples of GENERAL_CHAT (VERY LIMITED):
- "Hello" / "Hi" / "Hey"
- "Thank you" / "Thanks"
- "What is SQL?" (asking about SQL concept, not data)
- "How does this chatbot work?"

When in doubt, return NEEDS_DATABASE.

Return ONLY one word: either "NEEDS_DATABASE" or "GENERAL_CHAT"."""

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an intent classifier for a data query chatbot. "
                            "Your primary purpose is to help users query their database. "
                            "BIAS TOWARD 'NEEDS_DATABASE' - only use 'GENERAL_CHAT' for clear non-data questions like greetings or thanks. "
                            "If the user mentions ANY data-related words (show, find, products, sales, best, etc.), return 'NEEDS_DATABASE'. "
                            "Return ONLY 'NEEDS_DATABASE' or 'GENERAL_CHAT'."
                        )
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.0,  # More deterministic
                max_tokens=10
            )
            
            intent = response.choices[0].message.content.strip().upper()
            
            # Validate response
            if "NEEDS_DATABASE" in intent:
                logger.info(f"Intent classified as NEEDS_DATABASE for query: {user_query[:50]}")
                return "NEEDS_DATABASE"
            elif "GENERAL_CHAT" in intent:
                logger.info(f"Intent classified as GENERAL_CHAT for query: {user_query[:50]}")
                return "GENERAL_CHAT"
            else:
                # Default to NEEDS_DATABASE if unclear
                logger.warning(f"Unclear intent classification: {intent}. Defaulting to NEEDS_DATABASE")
                return "NEEDS_DATABASE"
                
        except Exception as e:
            logger.error(f"Error classifying intent: {e}")
            # Default to NEEDS_DATABASE to maintain backwards compatibility
            return "NEEDS_DATABASE"
    
    def general_chat(self, user_query: str, chat_history: str = "") -> str:
        """
        Handle general conversation without database queries.
        
        Args:
            user_query: User's question or message
            chat_history: Previous conversation context
            
        Returns:
            str: Conversational response
        """
        try:
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a helpful Data Assistant chatbot. Your main purpose is to help users query and "
                        "analyze their data using natural language.\n\n"
                        "Guidelines:\n"
                        "- Be friendly, polite, and concise\n"
                        "- If the user greets you, greet them back warmly\n"
                        "- If they ask for clarification about previous results, explain clearly\n"
                        "- If they ask about SQL or data concepts, provide brief, helpful explanations\n"
                        "- If they thank you, acknowledge politely\n"
                        "- Keep responses brief (2-3 sentences max)\n"
                        "- Always remind them you can help with data queries if relevant\n"
                        "- You have access to their uploaded data and can answer questions about it"
                    )
                }
            ]
            
            # Add chat history if available
            if chat_history:
                messages.append({
                    "role": "system",
                    "content": f"Previous conversation context:\n{chat_history}"
                })
            
            # Add user query
            messages.append({
                "role": "user",
                "content": user_query
            })
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.7,
                max_tokens=200
            )
            
            answer = response.choices[0].message.content.strip()
            logger.info("Generated general chat response")
            return answer
            
        except Exception as e:
            logger.error(f"Error in general chat: {e}")
            return "I'm here to help you query and analyze your data. Feel free to ask me any questions about your uploaded data!"
    
    
    def text_to_sql(
        self,
        user_question: str,
        schema: str,
        chat_history: str = "",
        recent_query_results: List[Dict] = None
    ) -> str:
        """
        Convert natural language question to SQL query.
        
        Args:
            user_question: User's question in natural language
            schema: Database schema information
            chat_history: Previous conversation context (optional)
            recent_query_results: Recent query results for context-aware follow-ups (optional)
            
        Returns:
            str: Generated SQL query
            
        Raises:
            Exception: If API call fails
        """
        prompt = self._build_sql_prompt(user_question, schema, chat_history, recent_query_results)
        
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
        chat_history: str = "",
        recent_query_results: List[Dict] = None
    ) -> str:
        """
        Retry SQL generation with corrective feedback when initial query returns 0 results.
        This helps fix hallucinated filter values.
        
        Args:
            failed_sql: The SQL query that returned empty results
            user_question: Original user question
            schema: Database schema with possible values
            chat_history: Previous conversation context
            recent_query_results: Recent query results for context (optional)
            
        Returns:
            str: Corrected SQL query
        """
        # Build context parts
        context_parts = []
        
        if recent_query_results:
            context_parts.append(self._build_query_results_context(recent_query_results))
            context_parts.append("")
        
        corrective_prompt = f"""The previous query returned 0 results. This likely means you used an invalid filter value.

FAILED QUERY:
{failed_sql}

ORIGINAL QUESTION:
{user_question}

{chr(10).join(context_parts)}

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
                            "IMPORTANT: "
                            "- Do NOT include the SQL query in your response. "
                            "- If there are no results, say so clearly. "
                            "- Provide clear, informative summaries of the data. "
                            "- ALWAYS list specific details for the top 5-10 items found (Name, Price, etc.) as a bulleted list or table."
                            "- If results are broad, suggest ONE specific refinement (e.g. price/feature)."
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
    
    def describe_data_rows(
        self,
        user_question: str,
        sql_result: List[tuple],
        column_names: List[str],
        max_rows: int = 10
    ) -> str:
        """
        Generate a descriptive, user-friendly narrative of the data rows.
        Takes top N rows and describes them in natural language.
        
        Args:
            user_question: Original user question
            sql_result: Query results as list of tuples
            column_names: Column names from the result
            max_rows: Maximum number of rows to describe (default: 10)
            
        Returns:
            str: Descriptive narrative of the data
            
        Raises:
            Exception: If API call fails
        """
        if not sql_result or not column_names:
            return "No data available to describe."
        
        # Take only top N rows
        rows_to_describe = sql_result[:max_rows]
        
        # Build data representation
        data_text = f"Question: {user_question}\n\n"
        data_text += f"Column Names: {', '.join(column_names)}\n\n"
        data_text += f"Data ({len(rows_to_describe)} rows):\n"
        
        for i, row in enumerate(rows_to_describe, 1):
            row_dict = dict(zip(column_names, row))
            data_text += f"\nRow {i}:\n"
            for col, val in row_dict.items():
                # EXCLUDE EMBEDDINGS from narrative
                if 'embedding' in col.lower():
                    continue
                data_text += f"  - {col}: {val}\n"
        
        if len(sql_result) > max_rows:
            data_text += f"\n... and {len(sql_result) - max_rows} more rows not shown"
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a data storyteller. Your job is to describe database results in a natural, "
                            "conversational, and user-friendly way.\n\n"
                            "RULES:\n"
                            "1. Write in a narrative, descriptive style - like you're explaining to a colleague\n"
                            "2. Highlight interesting patterns, trends, or notable items\n"
                            "3. Use bullet points or numbered lists for clarity\n"
                            "4. Make it easy to read and understand\n"
                            "5. If there are prices, brands, categories, names - emphasize those\n"
                            "6. Keep it concise but informative (3-5 sentences per item maximum)\n"
                            "7. Group similar items if appropriate\n\n"
                            "Example good output:\n"
                            "'Here are the top products found:\n"
                            "1. **Nike Air Max 90** (ID: FW0005) - A footwear item in the Men's category, priced at $130.00\n"
                            "2. **Nike React Infinity** (ID: FW0006) - Another footwear option, also for Men, at $160.00\n"
                            "3. **Nike Dri-FIT Shirt** (ID: AP0023) - An apparel item in Men's category for $45.00'"
                        )
                    },
                    {
                        "role": "user",
                        "content": f"Describe this data in a friendly, readable way:\n\n{data_text}"
                    }
                ],
                temperature=0.4,
                max_tokens=600
            )
            
            description = response.choices[0].message.content.strip()
            logger.info(f"Generated descriptive narrative for {len(rows_to_describe)} rows")
            return description
            
        except Exception as e:
            logger.error(f"Error generating data description: {e}")
            # Fallback to simple formatting
            fallback = f"Showing {len(rows_to_describe)} rows:\n"
            for i, row in enumerate(rows_to_describe, 1):
                fallback += f"{i}. {', '.join(str(v) for v in row)}\n"
            return fallback
    def _build_query_results_context(self, recent_queries: List[Dict]) -> str:
        """
        Build context string from recent query results.
        
        Args:
            recent_queries: List of dicts with {question, sql, results, columns}
            
        Returns:
            Formatted context string showing what data was previously retrieved
        """
        if not recent_queries:
            return ""
        
        # Only use the MOST RECENT query to save tokens
        latest_query = recent_queries[-1:]
        
        parts = ["=== RECENT QUERY CONTEXT (for reference) ==="]
        parts.append("The user previously queried the following data. You can reference this in your SQL if the current question refers to 'these', 'those', or 'previous results'.\n")
        
        for i, query_data in enumerate(latest_query, 1):
            parts.append(f"Most Recent Query:")
            parts.append(f"  User asked: \"{query_data['question']}\"")
            parts.append(f"  Results returned ({len(query_data['results'])} rows):")
            
            # Format ONLY first 3 rows
            for row_idx, row in enumerate(query_data['results'][:3], 1):
                row_items = []
                for col, val in zip(query_data['columns'], row):
                    # EXCLUDE EMBEDDINGS (User Request - Critical for tokens)
                    if 'embedding' in col.lower():
                        continue
                        
                    # Aggressively truncate long values
                    val_str = str(val)[:30]
                    
                    # Skip empty/null values to save tokens
                    if val_str and val_str.lower() != 'none':
                        row_items.append(f"{col}={val_str}")
                row_str = ", ".join(row_items)
                parts.append(f"    Row {row_idx}: {row_str}")
            
            if len(query_data['results']) > 3:
                parts.append(f"    ... ({len(query_data['results']) - 3} more rows)")
            parts.append("")
        
        parts.append("=== END CONTEXT ===\n")
        return "\n".join(parts)
    
    def _build_sql_prompt(
        self,
        user_question: str,
        schema: str,
        chat_history: str,
        recent_query_results: List[Dict] = None
    ) -> str:
        """Build prompt for SQL generation with optional recent query results context."""
        parts = []
        
        # Add recent query results context FIRST (most important for follow-ups)
        if recent_query_results:
            results_context = self._build_query_results_context(recent_query_results)
            parts.append(results_context)
            parts.append("")
        
        # Then add chat history
        if chat_history:
            parts.append(chat_history)
            parts.append("")
        
        parts.append(schema)
        parts.append("")
        parts.append(f"USER QUESTION: {user_question}")
        parts.append("")
        parts.append("Generate a SQL query to answer this question.")
        if recent_query_results:
            parts.append("IMPORTANT: If the question refers to previous results (like 'these', 'those', 'among them'), use the RECENT QUERY CONTEXT above to understand what data the user is referring to.")
        
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
                # Safe truncate string representation of each value to prevent huge embeddings
                # Since column names might be missing, this heuristic saves us.
                safe_row = [str(val)[:100] for val in row]
                parts.append(f"Row {i+1}: {tuple(safe_row)}")
            
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
