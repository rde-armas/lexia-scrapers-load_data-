from typing import Any, Dict, List, Optional
import numpy as np
import httpx
import asyncio
import os
from dotenv import load_dotenv
from services.base import Service, Result

# Load environment variables
load_dotenv()

# Constants
MAX_TOKENS_MODEL: int = 8192
MAX_TOKENS_CHUNKING: int = 512
OVERLAP: float = 0.2
CHARS_PER_TOKEN: float = 2.5

NOVITA_TOKEN = os.getenv("NOVITA")

class EmbeddingProvider:
    @staticmethod
    async def fetch_async(texts: list[str]) -> list[list[float]]:
        """Fetch embeddings from Novita AI via OpenAI compatible endpoint with batching."""
        print(f"[EMBEDDING_PROVIDER] Fetching embeddings for {len(texts)} texts via Novita OpenAI-compatible API")
        
        if not NOVITA_TOKEN:
            print("[EMBEDDING_PROVIDER] Novita API token (NOVITA) missing")
            raise ValueError("Novita API token (NOVITA) missing")

        # OpenAI compatible endpoint for Novita AI
        url = "https://api.novita.ai/openai/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {NOVITA_TOKEN}",
            "Content-Type": "application/json"
        }
        
        BATCH_SIZE = 20
        all_embeddings = []
        
        async with httpx.AsyncClient() as client:
            for i in range(0, len(texts), BATCH_SIZE):
                batch = texts[i:i+BATCH_SIZE]
                print(f"[EMBEDDING_PROVIDER] Fetching batch {i//BATCH_SIZE + 1} of {(len(texts) + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch)} items)...")
                
                try:
                    response = await client.post(
                        url,
                        headers=headers,
                        json={
                            "model": "baai/bge-m3",
                            "input": batch
                        },
                        timeout=60.0
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    # OpenAI format: {"data": [{"embedding": [...], "index": 0}, ...]}
                    embeddings_data = data.get("data", [])
                    
                    # Ensure they are sorted by index if present
                    if embeddings_data and isinstance(embeddings_data[0], dict) and "index" in embeddings_data[0]:
                        embeddings_data.sort(key=lambda x: x.get("index", 0))
                    
                    # Extract embeddings. Handle both [{"embedding": [...]}, ...] and [[...], ...] formats
                    if embeddings_data and isinstance(embeddings_data[0], dict):
                        batch_embeddings = [item["embedding"] for item in embeddings_data]
                    else:
                        batch_embeddings = embeddings_data
                    
                    if not batch_embeddings:
                        print(f"[EMBEDDING_PROVIDER] No embeddings returned in response for batch: {data}")
                        raise ValueError("No embeddings returned from Novita AI")
                    
                    all_embeddings.extend(batch_embeddings)
                except Exception as e:
                    print(f"[EMBEDDING_PROVIDER] Error fetching Novita AI embeddings: {e}")
                    raise
                    
        return all_embeddings

    @staticmethod
    def fetch(texts: list[str]) -> list[list[float]]:
        """Synchronous wrapper for fetch_async using a fresh event loop or threading if needed."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        if loop.is_running():
            # If the loop is already running (e.g. in load_sentences.py), we can't call asyncio.run().
            # Instead, we run the coroutine in a separate thread to get its own loop.
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(asyncio.run, EmbeddingProvider.fetch_async(texts))
                return future.result()
            
        return asyncio.run(EmbeddingProvider.fetch_async(texts))


class EmbeddingService(Service):
    def __init__(
        self,
        record_id: int,
        record_type: str,
        documents: List[str],
        token_limit: int = MAX_TOKENS_MODEL,
    ):
        super().__init__()
        self.record_id = record_id
        self.record_type = record_type
        self.documents = documents
        self.token_limit = token_limit

    def _execute(self) -> None:
        print(
            f"[EMBEDDING] Generating embeddings for {len(self.documents)} documents of type {self.record_type} with {self.token_limit} tokens limit"
        )
        
        if not self.documents or all(not doc.strip() for doc in self.documents):
            print("[EMBEDDING] ERROR: No valid documents to process!")
            self.result.add_error("No valid documents to process")
            return
            
        try:
            embeddings: list[list[float]] = EmbeddingProvider.fetch(self.documents)
            
            self.result["record_id"] = self.record_id
            self.result["record_type"] = self.record_type
            self.result[f"{self.record_type}"] = self.documents
            self.result["embeddings"] = embeddings
            
            print(f"[EMBEDDING] Successfully generated {len(embeddings)} embeddings for {len(self.documents)} documents")
        except Exception as e:
            error_msg = f"Error generating embeddings via Novita: {e}"
            print(f"[EMBEDDING] {error_msg}")
            self.result.add_error(error_msg)

    async def run_async(self) -> Result:
        """Asynchronous entry point for the service."""
        print(
            f"[EMBEDDING] Generating embeddings ASYNC for {len(self.documents)} documents of type {self.record_type}"
        )
        try:
            embeddings = await EmbeddingProvider.fetch_async(self.documents)
            
            if embeddings is not None:
                self.result["record_id"] = self.record_id
                self.result["record_type"] = self.record_type
                self.result[f"{self.record_type}"] = self.documents
                self.result["embeddings"] = embeddings
            return self.result
        except Exception as e:
            error_msg = f"Error generating embeddings via Novita (async): {e}"
            print(f"[EMBEDDING] {error_msg}")
            self.result.add_error(error_msg)
            return self.result

    def split_document(
        self, text: str, max_tokens: int = MAX_TOKENS_CHUNKING, overlap: float = OVERLAP
    ) -> List[str]:
        max_chars = int(max_tokens * CHARS_PER_TOKEN)
        overlap_chars = int(max_chars * overlap)
        step = max_chars - overlap_chars
        
        chunks = []
        for i in range(0, len(text), step):
            chunk = text[i : i + max_chars]
            if chunk.strip():  # Filter out empty or whitespace-only strings
                chunks.append(chunk)
        
        return chunks


class BaseIngestor(EmbeddingService):
    def __init__(
        self,
        record_id: int,
        record_type: str,
        text: str,
        token_limit: int,
        chunking_params: Optional[Dict] = None,
        force_chunking: bool = False,
    ) -> None:
        if force_chunking or self.exceeds_token_limit(text, token_limit):
            print(
                "[EMBEDDING] Document exceeds token limit or force_chunking=True, applying chunking"
            )
            if chunking_params:
                documents: list[str] = self.split_document(text, **chunking_params)
            else:
                documents = self.split_document(
                    text, max_tokens=MAX_TOKENS_CHUNKING, overlap=OVERLAP
                )
        else:
            documents = [text]

        super().__init__(record_id, record_type, documents, token_limit)

    def exceeds_token_limit(self, text: str, token_limit: int) -> bool:
        estimated_tokens = int(len(text) / CHARS_PER_TOKEN)
        exceeds: bool = estimated_tokens > token_limit
        if exceeds:
            print(
                f"[EMBEDDING] Document exceeds token limit: ~{estimated_tokens} tokens (estimated) > {token_limit}"
            )
        return exceeds


class TextIngestorService(BaseIngestor):
    def __init__(
        self,
        text,
        record_id=0,
        record_type="text",
        token_limit: int = MAX_TOKENS_MODEL,
        chunking_params: Optional[Dict] = None,
        force_chunking: bool = False,
    ) -> None:
        print(
            f"[EMBEDDING] Initializing service for generic text, force_chunking={force_chunking}"
        )
        super().__init__(
            record_id=record_id,
            record_type=record_type,
            text=text,
            token_limit=token_limit,
            chunking_params=chunking_params,
            force_chunking=force_chunking,
        )
