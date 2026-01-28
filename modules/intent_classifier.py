"""
Intent Classifier Module  
LLM-based query decomposition into strict JSON for hybrid search.
"""

import logging
import json
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class IntentResult:
    """Structured intent classification result."""
    intent_type: str  # product_search | recommendation | comparison | explanation | out_of_scope
    sql_filters: str  # SQL WHERE clause or "TRUE"
    semantic_query: str  # Natural language semantic description
    requires_clarification: bool
    raw_json: Dict  # Original JSON response


class IntentClassifier:
    """Intent classification and query decomposition using LLM."""
    
    def __init__(self, llm_client):
        """
        Initialize intent classifier.
        
        Args:
            llm_client: LLM client instance for API calls
        """
        self.llm_client = llm_client
    
    def decompose_query(
        self,
        user_query: str,
        schema: str,
        chat_history: str = ""
    ) -> Tuple[bool, Optional[IntentResult], Optional[str]]:
        """
        Decompose user query into structured intent.
        
        Args:
            user_query: User's natural language query
            schema: Database schema information
            chat_history: Previous conversation context
            
        Returns:
            Tuple[bool, Optional[IntentResult], Optional[str]]: 
                (success, intent_result, error_message)
        """
        try:
            # Build decomposition prompt
            prompt = self._build_decomposition_prompt(user_query, schema, chat_history)
            
            # Call LLM
            response = self.llm_client.client.chat.completions.create(
                model=self.llm_client.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a query decomposition assistant. Output ONLY valid JSON, no other text."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistent JSON
                max_tokens=500
            )
            
            # Extract response
            json_text = response.choices[0].message.content.strip()
            
            # Parse JSON
            intent_data = self._parse_json_response(json_text)
            
            if intent_data is None:
                return False, None, "Failed to parse LLM response as JSON"
            
            # Validate and create IntentResult
            intent_result = self._validate_intent_data(intent_data)
            
            if intent_result is None:
                return False, None, "Invalid intent structure"
            
            logger.info(f"✅ Intent classified: {intent_result.intent_type}")
            return True, intent_result, None
        
        except Exception as e:
            logger.error(f"Error decomposing query: {e}")
            return False, None, str(e)
    
    def _build_decomposition_prompt(
        self,
        user_query: str,
        schema: str,
        chat_history: str
    ) -> str:
        """Build prompt for intent decomposition."""
        
        prompt = f"""You are a query decomposition assistant for a product search chatbot. Analyze the user query and decompose it into STRICT JSON.

USER QUERY: "{user_query}"

{"CHAT HISTORY (for understanding context):" if chat_history else ""}
{chat_history if chat_history else ""}

DATABASE SCHEMA:
{schema}

Output ONLY valid JSON with this EXACT structure:
{{
  "intent_type": "<one of: product_search | recommendation | comparison | explanation | out_of_scope>",
  "sql_filters": "<valid SQL WHERE clause OR 'TRUE' if no filters>",
  "semantic_query": "<natural language semantic description OR empty string>",
  "requires_clarification": <true OR false>
}}

CRITICAL RULES:
1. intent_type MUST be exactly one of: product_search, recommendation, comparison, explanation, out_of_scope
2. sql_filters MUST be valid SQL for a WHERE clause. Use column names from schema. Use 'TRUE' if no specific filters needed.
3. semantic_query should capture the semantic/subjective aspects (comfort, style, quality) in natural language. NO SQL syntax here.
4. requires_clarification should be true ONLY if the query is too vague/ambiguous to process.
5. For follow-up queries like "cheaper ones" or "show me more", use chat history to infer context.

EXAMPLES:

Query: "Comfortable shoes for standing all day"
{{
  "intent_type": "product_search",
  "sql_filters": "TRUE",
  "semantic_query": "comfortable shoes for standing all day with good cushioning and support",
  "requires_clarification": false
}}

Query: "Show me Nike shoes under 5000"
{{
  "intent_type": "product_search",
  "sql_filters": "brand = 'Nike' AND price < 5000",
  "semantic_query": "",
  "requires_clarification": false
}}

Query: "What's the weather?"
{{
  "intent_type": "out_of_scope",
  "sql_filters": "TRUE",
  "semantic_query": "",
  "requires_clarification": false
}}

Query: "Show me something nice"
{{
  "intent_type": "product_search",
  "sql_filters": "TRUE",
  "semantic_query": "",
  "requires_clarification": true
}}

NOW ANALYZE THE USER QUERY AND RESPOND WITH JSON ONLY."""

        return prompt
    
    def _parse_json_response(self, response_text: str) -> Optional[Dict]:
        """
        Parse JSON from LLM response, handling markdown code blocks.
        
        Args:
            response_text: Raw LLM response
            
        Returns:
            Optional[Dict]: Parsed JSON or None
        """
        try:
            # Remove markdown code blocks if present
            text = response_text.strip()
            
            if text.startswith("```json"):
                text = text[7:]  # Remove ```json
            elif text.startswith("```"):
                text = text[3:]  # Remove ```
            
            if text.endswith("```"):
                text = text[:-3]  # Remove closing ```
            
            text = text.strip()
            
            # Parse JSON
            return json.loads(text)
        
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            logger.error(f"Response text: {response_text}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing JSON: {e}")
            return None
    
    def _validate_intent_data(self, data: Dict) -> Optional[IntentResult]:
        """
        Validate intent data and create IntentResult.
        
        Args:
            data: Parsed JSON data
            
        Returns:
            Optional[IntentResult]: Validated intent result or None
        """
        try:
            # Check required fields
            required_fields = ["intent_type", "sql_filters", "semantic_query", "requires_clarification"]
            
            for field in required_fields:
                if field not in data:
                    logger.error(f"Missing required field: {field}")
                    return None
            
            # Validate intent_type
            valid_intents = ["product_search", "recommendation", "comparison", "explanation", "out_of_scope"]
            intent_type = data["intent_type"]
            
            if intent_type not in valid_intents:
                logger.error(f"Invalid intent_type: {intent_type}")
                return None
            
            # Create IntentResult
            return IntentResult(
                intent_type=intent_type,
                sql_filters=str(data["sql_filters"]).strip(),
                semantic_query=str(data["semantic_query"]).strip(),
                requires_clarification=bool(data["requires_clarification"]),
                raw_json=data
            )
        
        except Exception as e:
            logger.error(f"Error validating intent data: {e}")
            return None


def get_intent_classifier(llm_client) -> IntentClassifier:
    """
    Get intent classifier instance.
    
    Args:
        llm_client: LLM client for API calls
        
    Returns:
        IntentClassifier: Intent classifier instance
    """
    return IntentClassifier(llm_client)
