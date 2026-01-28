"""
Guardrails Module
Handles clarification, fallbacks, and out-of-scope queries.
"""

import logging
from typing import Optional, List, Dict, Tuple
from .intent_classifier import IntentResult

logger = logging.getLogger(__name__)


class Guardrails:
    """Production-grade guardrails for hybrid search system."""
    
    @staticmethod
    def check_clarification_needed(intent_result: IntentResult) -> Optional[str]:
        """
        Check if query requires clarification.
        
        Args:
            intent_result: Intent classification result
            
        Returns:
            Optional[str]: Clarification message or None
        """
        if intent_result.requires_clarification:
            logger.info("⚠️ Query requires clarification")
            
            return """I'd be happy to help you find the right product! Could you provide a bit more detail? For example:

- What type of product are you looking for?
- Any specific features or requirements?
- Price range you have in mind?
- Preferred brand or style?

The more specific you are, the better I can assist you!"""
        
        return None
    
    @staticmethod
    def handle_out_of_scope(user_query: str, intent_result: IntentResult) -> Optional[str]:
        """
        Handle out-of-scope queries.
        
        Args:
            user_query: Original user query
            intent_result: Intent classification result
            
        Returns:
            Optional[str]: Out-of-scope message or None
        """
        if intent_result.intent_type == "out_of_scope":
            logger.info(f"⚠️ Out-of-scope query: {user_query}")
            
            return """I'm a specialized product search assistant, and I can help you with:

✅ Finding products
✅ Comparing options
✅ Getting recommendations
✅ Answering product-related questions

Unfortunately, I can't help with queries outside of product search. Is there anything product-related I can assist you with?"""
        
        return None
    
    @staticmethod
    def handle_zero_results(
        user_query: str,
        intent_result: IntentResult,
        table_name: str
    ) -> str:
        """
        Handle zero results with helpful feedback.
        
        Args:
            user_query: Original user query
            intent_result: Intent classification result
            table_name: Table that was searched
            
        Returns:
            str: Helpful fallback message
        """
        logger.info("⚠️ Zero results returned")
        
        message = f"""I couldn't find any products that match "{user_query}".

**Possible reasons**:
- The specific filters might be too restrictive
- The product might not be available in our catalog

**Suggestions**:
- Try broader search terms
- Remove some filters (like price range or brand)
- Ask me to "show all {table_name.replace('_', ' ')}"
- Try a different query

Would you like me to suggest some alternatives?"""
        
        return message
    
    @staticmethod
    def check_similarity_threshold(
        results: List[Dict],
        threshold: float = 0.3
    ) -> bool:
        """
        Check if results meet minimum similarity threshold.
        
        Args:
            results: Search results with similarity_score
            threshold: Minimum similarity threshold
            
        Returns:
            bool: True if results meet threshold
        """
        if not results:
            return False
        
        # Check if top result meets threshold
        top_score = results[0].get('similarity_score', 0)
        
        if top_score < threshold:
            logger.warning(f"⚠️ Top similarity score ({top_score:.3f}) below threshold ({threshold})")
            return False
        
        return True
    
    @staticmethod
    def suggest_rephrase(similarity_score: float) -> Optional[str]:
        """
        Suggest rephrasing if similarity is low.
        
        Args:
            similarity_score: Top result similarity score
            
        Returns:
            Optional[str]: Suggestion message or None
        """
        if 0.2 <= similarity_score < 0.3:
            return "\n\n💡 **Tip**: The results might not exactly match what you're looking for. Try rephrasing your query or being more specific for better matches!"
        
        return None
    
    @staticmethod
    def validate_sql_filters(sql_filters: str, column_names: List[str]) -> Tuple[bool, str]:
        """
        Basic validation of SQL filters for safety.
        
        Args:
            sql_filters: SQL WHERE clause
            column_names: Valid column names from schema
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        # Check for SQL injection patterns
        dangerous_keywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE', '--', ';']
        
        upper_filters = sql_filters.upper()
        
        for keyword in dangerous_keywords:
            if keyword in upper_filters:
                logger.error(f"🚨 Dangerous SQL keyword detected: {keyword}")
                return False, f"Invalid SQL: dangerous keyword '{keyword}' not allowed"
        
        # Allow TRUE as a special case
        if sql_filters.strip().upper() == 'TRUE':
            return True, ""
        
        # Basic syntax check (this is simplified - LLM should generate safe SQL)
        if not any(op in sql_filters for op in ['=', '>', '<', 'LIKE', 'IN', 'BETWEEN', 'AND', 'OR']):
            logger.warning("⚠️ SQL filters might be malformed")
            return False, "SQL filters appear malformed"
        
        return True, ""


def get_guardrails() -> Guardrails:
    """
    Get guardrails instance.
    
    Returns:
        Guardrails: Guardrails instance
    """
    return Guardrails()
