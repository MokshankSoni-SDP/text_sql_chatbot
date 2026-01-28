"""
Embedding Service Module
Centralized embedding generation using Sentence Transformers.
Model: sentence-transformers/all-MiniLM-L6-v2 (384-dim, CPU-friendly)
"""

import logging
from typing import List, Optional
import numpy as np

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Embedding generation service using sentence-transformers."""
    
    _instance = None
    _model = None
    
    def __new__(cls):
        """Singleton pattern to ensure model loads only once."""
        if cls._instance is None:
            cls._instance = super(EmbeddingService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize embedding service."""
        if self._model is None:
            self._load_model()
    
    def _load_model(self):
        """Load sentence-transformers model."""
        try:
            from sentence_transformers import SentenceTransformer
            
            logger.info("Loading sentence-transformers model: all-MiniLM-L6-v2")
            self._model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
            logger.info(f"✅ Model loaded successfully. Embedding dimension: {self.get_embedding_dim()}")
            
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            raise RuntimeError(f"Could not initialize embedding service: {e}")
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text.
        
        Args:
            text: Input text
            
        Returns:
            List[float]: 384-dimensional embedding vector
        """
        if not text or not text.strip():
            # Return zero vector for empty text
            return [0.0] * self.get_embedding_dim()
        
        try:
            embedding = self._model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            # Return zero vector on failure
            return [0.0] * self.get_embedding_dim()
    
    def generate_batch_embeddings(self, texts: List[str], batch_size: int = 32) -> List[List[float]]:
        """
        Generate embeddings for multiple texts (batch processing).
        
        Args:
            texts: List of input texts
            batch_size: Batch size for processing
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        if not texts:
            logger.warning("⚠️ Empty text list provided for batch embedding")
            return []
        
        try:
            logger.debug(f"Processing {len(texts)} texts in batches of {batch_size}")
            
            # Filter out empty texts but maintain indices
            valid_indices = []
            valid_texts = []
            empty_count = 0
            
            for i, text in enumerate(texts):
                if text and text.strip():
                    valid_indices.append(i)
                    valid_texts.append(text)
                else:
                    empty_count += 1
            
            if empty_count > 0:
                logger.debug(f"Found {empty_count} empty texts, will use zero vectors")
            
            # Generate embeddings for valid texts
            if valid_texts:
                logger.debug(f"Encoding {len(valid_texts)} valid texts...")
                embeddings = self._model.encode(
                    valid_texts,
                    batch_size=batch_size,
                    show_progress_bar=len(valid_texts) > 100,
                    convert_to_numpy=True
                )
                logger.debug(f"✅ Successfully encoded {len(valid_texts)} texts")
            else:
                logger.warning("⚠️ No valid texts to encode, returning zero vectors")
                embeddings = []
            
            # Reconstruct full list with zero vectors for empty texts
            result = []
            zero_vector = [0.0] * self.get_embedding_dim()
            embedding_idx = 0
            
            for i in range(len(texts)):
                if i in valid_indices:
                    result.append(embeddings[embedding_idx].tolist())
                    embedding_idx += 1
                else:
                    result.append(zero_vector)
            
            logger.debug(f"✅ Batch complete: {len(result)} embeddings generated")
            return result
        
        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR in batch embedding generation: {e}", exc_info=True)
            logger.error(f"Error details: texts count={len(texts)}, batch_size={batch_size}")
            # Return zero vectors on failure
            logger.warning("⚠️ Returning zero vectors due to error")
            return [[0.0] * self.get_embedding_dim() for _ in texts]
    
    def get_embedding_dim(self) -> int:
        """
        Get embedding dimension.
        
        Returns:
            int: Embedding dimension (384 for all-MiniLM-L6-v2)
        """
        if self._model is None:
            return 384  # Default for all-MiniLM-L6-v2
        
        try:
            return self._model.get_sentence_embedding_dimension()
        except:
            return 384
    
    def compute_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """
        Compute cosine similarity between two embeddings.
        
        Args:
            embedding1: First embedding vector
            embedding2: Second embedding vector
            
        Returns:
            float: Cosine similarity (0 to 1, higher = more similar)
        """
        try:
            vec1 = np.array(embedding1)
            vec2 = np.array(embedding2)
            
            # Cosine similarity
            similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
            
            # Normalize to 0-1 range
            return float((similarity + 1) / 2)
        
        except Exception as e:
            logger.error(f"Error computing similarity: {e}")
            return 0.0


# Global instance
_embedding_service_instance = None


def get_embedding_service() -> EmbeddingService:
    """
    Get embedding service instance (singleton).
    
    Returns:
        EmbeddingService: Embedding service instance
    """
    global _embedding_service_instance
    
    if _embedding_service_instance is None:
        _embedding_service_instance = EmbeddingService()
    
    return _embedding_service_instance


def generate_embedding(text: str) -> List[float]:
    """
    Convenience function to generate embedding.
    
    Args:
        text: Input text
        
    Returns:
        List[float]: 384-dimensional embedding
    """
    service = get_embedding_service()
    return service.generate_embedding(text)


def generate_batch_embeddings(texts: List[str]) -> List[List[float]]:
    """
    Convenience function to generate batch embeddings.
    
    Args:
        texts: List of input texts
        
    Returns:
        List[List[float]]: List of embeddings
    """
    service = get_embedding_service()
    return service.generate_batch_embeddings(texts)
