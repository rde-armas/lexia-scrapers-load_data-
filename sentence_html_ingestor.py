from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from bs4 import BeautifulSoup

from services.service import TextIngestorService


class SentenceHTMLIngestor():
    def __init__(
        self, chunking_params: Optional[Dict] = None, force_chunking: bool = False
    ):
        self.chunking_params = chunking_params
        self.force_chunking = force_chunking

    def get_content_type(self) -> str:
        return "text/html"

    def ingest_file(self, file: Path) -> Optional[Dict[str, Any]]:

        if not file.exists():
            print(f"[INGESTOR] Ruta de archivo no existente: {file}")
            return None

        print(f"[INGESTOR] Ingiriendo archivo HTML: {file.name}")

        try:
            processed_data = self._parse_html_to_structured_data(file)
            main_text = processed_data.get("text")

            if main_text and self.force_chunking:
                print(f"[INGESTOR] Generando embeddings para {file.name}...")

                embedding_chunks_service = TextIngestorService(
                    text=main_text,
                    record_id=processed_data["id"],
                    record_type="chunked_sentence",
                    chunking_params=self.chunking_params,
                    force_chunking=self.force_chunking,
                )

                chunks_embedding_result = embedding_chunks_service.run()

                if chunks_embedding_result.success():
                    chunks = chunks_embedding_result.data.get("chunked_sentence", [])
                    embeddings = chunks_embedding_result.data.get("embeddings", [])
                    processed_data["short_embeddings_attributes"] = [
                        {"chunk": chunk, "embedding_type": "short", "vector": vector}
                        for chunk, vector in zip(chunks, embeddings)
                    ]
                else:
                    print(
                        f"[INGESTOR] Error al generar embeddings cortos para {file.name}: {chunks_embedding_result.errors()}"
                    )
                    processed_data["short_embeddings_attributes"] = []

                full_embedding_service = TextIngestorService(
                    text=main_text,
                    record_id=processed_data["id"],
                    record_type="sentence",
                    force_chunking=False,
                )
                full_result = full_embedding_service.run()

                if full_result.success():
                    full_text_chunk = [main_text]
                    full_text_embeddings = full_result.data.get("embeddings", [])
                    processed_data["long_embeddings_attributes"] = [
                        {"chunk": chunk, "embedding_type": "long", "vector": vector}
                        for chunk, vector in zip(full_text_chunk, full_text_embeddings)
                    ]
                else:
                    print(
                        f"[INGESTOR] Error al generar embeddings largos para {file.name}: {full_result.errors()}"
                    )
                    processed_data["long_embeddings_attributes"] = []

                print(
                    f"[INGESTOR] Embeddings procesados para {file.name}"
                )
            else:
                logger.warning(
                    f"[INGESTOR] No hay contenido principal para embedding en {file.name}, omitiendo."
                )
                processed_data["short_embeddings_attributes"] = []
                processed_data["long_embeddings_attributes"] = []

            print(
                f"[INGESTOR] Procesado exitosamente {file.name}, id: {processed_data['id']}"
            )
            return processed_data

        except Exception as e:
            print(
                f"[INGESTOR] Ocurrió un error inesperado al ingerir {file.name}: {e}"
            )
            return None

    def get_ingestor_name(self) -> str:
        return "sentence_html_ingestor"

    def can_ingest(self, file_path: Path, content_type: Optional[str] = None) -> bool:
        return file_path.suffix.lower() == ".html"

    def _parse_html_to_structured_data(self, html_path: Path) -> Dict[str, Any]:
        with open(html_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")

        data: Dict[str, Any] = {"id": html_path.stem}

        first_table = soup.find("table", id="j_id3")
        if first_table:
            cells = first_table.find_all("td")
            data["number"] = cells[0].get_text(strip=True) if len(cells) > 0 else None
            data["court"] = cells[1].get_text(strip=True) if len(cells) > 1 else None
            data["importance"] = (
                cells[2].get_text(strip=True) if len(cells) > 2 else None
            )
            data["sentence_type"] = (
                cells[3].get_text(strip=True) if len(cells) > 3 else None
            )
        else:
            data["number"] = data["court"] = data["importance"] = data[
                "sentence_type"
            ] = None

        second_table = soup.find("table", id="j_id21")
        if second_table:
            cells = second_table.find_all("td")
            data["date_str"] = cells[0].get_text(strip=True) if len(cells) > 0 else None
            data["file_number"] = (
                cells[1].get_text(strip=True) if len(cells) > 1 else None
            )
            data["procedure"] = (
                cells[2].get_text(strip=True) if len(cells) > 2 else None
            )
            if data["date_str"]:
                try:
                    data["date"] = (
                        datetime.strptime(data["date_str"], "%d/%m/%Y")
                        .date()
                        .isoformat()
                    )
                except ValueError:
                    data["date"] = None
            else:
                data["date"] = None
        else:
            data["date_str"] = data["file_number"] = data["procedure"] = data[
                "date"
            ] = None

        subjects_table = soup.find("table", id="j_id35")
        subjects = []
        if subjects_table:
            subjects = [td.get_text(strip=True) for td in subjects_table.find_all("td")]
        data["subjects"] = subjects

        signatories_table = soup.find("table", id="gridFirmantes")
        signatories = []
        if signatories_table:
            tbody = signatories_table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 2:
                        signatories.append(
                            {
                                "name": cols[0].get_text(strip=True),
                                "role": cols[1].get_text(strip=True),
                            }
                        )
        data["signatories"] = signatories

        editors_table = soup.find("table", id="gridRedactores")
        editors = []
        if editors_table:
            tbody = editors_table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 2:
                        editors.append(
                            {
                                "name": cols[0].get_text(strip=True),
                                "role": cols[1].get_text(strip=True),
                            }
                        )
        data["editors"] = editors

        discordants_table = soup.find("table", id="gridDiscordes")
        discordants = []
        if discordants_table:
            tbody = discordants_table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 2:
                        discordants.append(
                            {
                                "name": cols[0].get_text(strip=True),
                                "role": cols[1].get_text(strip=True),
                            }
                        )
        data["discordants"] = discordants

        descriptors_table = soup.find("table", id="j_id77")
        descriptors = []
        if descriptors_table:
            tbody = descriptors_table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 2:
                        descriptors.append(
                            {
                                "path": cols[0].get_text(strip=True),
                                "abstract": cols[1].get_text(strip=True),
                            }
                        )
        data["descriptors"] = descriptors

        summary_table = soup.find("table", id="j_id107")
        if summary_table:
            td = summary_table.find("td")
            data["summary"] = td.get_text(strip=True) if td else None
        else:
            data["summary"] = None

        text_div = soup.find("div", id="panelTextoSent_body")

        if text_div:
            texto_box = text_div.find("span", id="textoSentenciaBox")
            if texto_box:
                data["text"] = texto_box.get_text(separator="\n", strip=True)
                data["raw_text"] = str(texto_box)
            else:
                search_results = text_div.find("span", id="searchResults")
                if search_results:
                    data["text"] = search_results.get_text(separator="\n", strip=True)
                    data["raw_text"] = str(search_results)
                else:
                    data["text"] = text_div.get_text(separator="\n", strip=True)
                    data["raw_text"] = str(text_div)
        else:
            data["text"] = None
            data["raw_text"] = None

        data["created_at_parser"] = datetime.now().isoformat()

        return data
