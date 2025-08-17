from typing import Any, Dict, List, Optional

import numpy as np
import requests
from transformers import AutoTokenizer

from services.base import Service, Result

MAX_TOKENS_MODEL= 8192
MAX_TOKENS_CHUNKING= 512
OVERLAP= 0.2
NOVITA_EMBEDDING_API_URL: str = "https://api.novita.ai/v3/openai/embeddings"
NOVITA_API_KEY = "sk_mJLGD4OndbLCV4ZDsYQ-le99dBZbaIKXePG01uuy1xI"

tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")


class IngestorService(Service):
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
        embeddings: list[list[float]] = self.generate_embeddings(self.documents)

        self.result["record_id"] = self.record_id
        self.result["record_type"] = self.record_type
        self.result[f"{self.record_type}"] = self.documents
        self.result["embeddings"] = embeddings

    def generate_embeddings(self, document_list: List[str]) -> List[List[float]]:
        try:
            headers = {
                "Authorization": f"Bearer {NOVITA_API_KEY}",
                "Content-Type": "application/json",
            }
            data = {
                "input": document_list,
                "model": "baai/bge-m3",
                "encoding_format": "float",
            }
            response = requests.post(
                NOVITA_EMBEDDING_API_URL,
                headers=headers,
                json=data,
            )
            if response.status_code != 200:
                print(
                    f"[EMBEDDING] Service error: {response.status_code} - {response.text}"
                )
                raise RuntimeError(f"Embedding service error: {response.status_code}")

            response_json = response.json()
            embeddings_list = [item["embedding"] for item in response_json["data"]]
            return embeddings_list

        except Exception as e:
            print(f"[EMBEDDING] Error generating embeddings: {e}")
            raise RuntimeError(f"Error generating embeddings: {e}")

    def split_document(
        self, text: str, max_tokens: int = MAX_TOKENS_CHUNKING, overlap: float = OVERLAP
    ) -> List[str]:
        tokens = tokenizer(text, return_tensors="pt")["input_ids"][0]
        token_chunks = []
        step = (max_tokens - 5) - int((max_tokens - 5) * overlap)

        for i in range(0, len(tokens), step):
            chunk_tokens = tokens[i : i + max_tokens - 5]
            chunk_text = tokenizer.decode(chunk_tokens, skip_special_tokens=True)
            if chunk_text.strip():  # Filter out empty or whitespace-only strings
                token_chunks.append(chunk_text)

        return token_chunks


class BaseIngestor(IngestorService):
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
        tokens = tokenizer(text, return_tensors="pt")["input_ids"][0]
        exceeds: bool = len(tokens) > token_limit
        if exceeds:
            print(
                f"[EMBEDDING] Document exceeds token limit: {len(tokens)} > {token_limit}"
            )
        return exceeds


class TextIngestorService(BaseIngestor):
    def __init__(
        self,
        text,
        record_id=0,
        record_type="text",
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
            token_limit=MAX_TOKENS_MODEL,
            chunking_params=chunking_params,
            force_chunking=force_chunking,
        )
