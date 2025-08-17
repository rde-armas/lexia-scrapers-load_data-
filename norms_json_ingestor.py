import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.service import TextIngestorService


class NormsJsonIngestor():
    def __init__(self):
        super().__init__()

    def get_content_type(self) -> str:
        return "application/json"

    @staticmethod
    def _format_date(date_str: str) -> str:
        """Helper to format date strings from DD/MM/YYYY to YYYY-MM-DD."""
        if not date_str or not isinstance(date_str, str):
            return ""
        try:
            dt_obj = datetime.strptime(date_str, "%d/%m/%Y")
            return dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            print(
                f"[NORMS INGESTOR] Could not parse date: {date_str}. Expected DD/MM/YYYY."
            )
            return ""

    def ingest_file(self, json_file_path: Path) -> Optional[Dict[str, Any]]:
        print(f"[NORMS INGESTOR] Ingesting {json_file_path.name}...")
        try:
            with open(json_file_path, "r", encoding="utf-8") as f:
                norm_json = json.load(f)

            transformed_data = {
                "norm_id": int(norm_json.get("nroNorma", "0")),
                "norm_type": norm_json.get("tipoNorma", "").strip(),
                "number": int(norm_json.get("nroNorma", "0")),
                "year": int(norm_json.get("anioNorma", "0")),
                "title": norm_json.get("nombreNorma", "").strip(),
                "hearings": norm_json.get("vistos", "").strip(),
                "references": "",
                "signers": norm_json.get("firmantes", "").strip(),
                "references_url": "",
                "impo_url": norm_json.get("urlVerImagen", "").strip(),
                "newspaper_image_url": "",
                "promulgated_at": self._format_date(
                    norm_json.get("fechaPromulgacion", "").strip()
                ),
                "published_at": self._format_date(
                    norm_json.get("fechaPublicacion", "").strip()
                ),
            }

            processed_articles = self._process_articles(norm_json, transformed_data)

            transformed_data["processed_articles"] = processed_articles
            print(
                f"[NORMS INGESTOR] Successfully ingested {json_file_path.name} with {len(processed_articles)} articles."
            )
            return transformed_data

        except Exception as e:
            print(
                f"[NORMS INGESTOR] Error ingesting {json_file_path.name}: {e}",
                exc_info=True,
            )
            return None

    def _process_articles(
        self, norm_json: Dict[str, Any], transformed_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        processed_articles = []
        articles = norm_json.get("articulos", [])
        if not articles:
            print(
                f"[NORMS INGESTOR] No articles found in norm {transformed_data.get('norm_id')}"
            )
            return []

        for art in articles:
            article_text = art.get("textoArticulo", "")
            if not article_text:
                continue

            try:
                embedding_service = TextIngestorService(
                    text=article_text,
                    record_id=art.get("urlArticulo"),
                    record_type="article",
                    force_chunking=False,
                )
                embedding_result = embedding_service.run()

                if not embedding_result.success():
                    error_details = embedding_result.errors()
                    print(
                        f"[NORMS INGESTOR] Embedding service failed for article in norm {transformed_data.get('norm_id')}: {error_details}"
                    )
                    continue

                ingested_texts = embedding_result["article"]
                ingested_embeddings_nested = embedding_result["embeddings"]

                if (
                    not ingested_embeddings_nested
                    or len(ingested_embeddings_nested) == 0
                ):
                    print(
                        f"[NORMS INGESTOR] Embedding service returned empty embedding for article in norm {transformed_data.get('norm_id')}"
                    )
                    continue

                ingested_text = (
                    ingested_texts[0]
                    if isinstance(ingested_texts, list)
                    else ingested_texts
                )
                ingested_embeddings = (
                    ingested_embeddings_nested[0]
                    if isinstance(ingested_embeddings_nested[0], list)
                    else ingested_embeddings_nested
                )

                ingested_text = str(ingested_text)

            except Exception as e_embed:
                print(
                    f"[NORMS INGESTOR] Exception during embedding generation for article in norm {transformed_data.get('norm_id')}: {e_embed}",
                    exc_info=True,
                )
                continue

            try:
                article_number = int(art.get("nroArticulo", "0"))
            except ValueError:
                article_number = 0

            processed_article = {
                "number": article_number,
                "title": art.get("tituloArticulo", "").strip(),
                "notes": art.get("notasArticulo", "").strip(),
                "references": art.get("secArticulo", "").strip(),
                "signers": transformed_data["signers"],
                "text": ingested_text,
                "references_url": art.get("urlArticulo", "").strip(),
                "impo_url": art.get("urlArticulo", "").strip(),
                "long_embeddings_attributes": [
                    {
                        "vector": ingested_embeddings,
                        "chunk": ingested_text,
                        "embedding_type": 1,
                    }
                ],
            }
            processed_articles.append(processed_article)

        return processed_articles
