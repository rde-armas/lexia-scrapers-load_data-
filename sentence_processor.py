import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from scraper import sentences_scraper
from sentence_html_ingestor import SentenceHTMLIngestor

API_URL = "http://api.lvh.me:3000/v1/sentences"

BASE_DATA_PATH = Path(
    os.getenv("LEXIA_BRAIN_DATA_PATH", Path(__file__).resolve().parent.parent / "data")
)
SENTENCES_HTML_BASE_DIR = BASE_DATA_PATH / "sentences"
SENTENCES_JSON_OUTPUT_DIR = BASE_DATA_PATH / "sentences_json"
PROCESSED_SENTENCES_DIR = BASE_DATA_PATH / "processed_sentences_html"

MAX_TOKENS_CHUNKING=512
OVERLAP=0.2


async def scrape_sentences_for_period(
    start_date: datetime, end_date: datetime, output_base_dir: Path
) -> List[Path]:
    print(
        f"[SENTENCES SCRAPING TASK] Initiating scraping for period: {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}"
    )
    output_base_dir.mkdir(parents=True, exist_ok=True)

    try:
        await sentences_scraper.scrape_sentences(
            session_start_date=start_date,
            session_end_date=end_date,
            output_dir=output_base_dir,
        )
        scraped_files = list(output_base_dir.glob("*.html"))
        print(
            f"[SENTENCES SCRAPING TASK] Scraping complete for period {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}. Found {len(scraped_files)} files in {output_base_dir}"
        )
        return scraped_files
    except Exception as e:
        print(
            f"[SENTENCES SCRAPING TASK] An error occurred during scraping for period {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}: {e}"
        )
        return []


def process_single_sentence_html(
    html_file_path: Path, sentence_ingestor_chunked: SentenceHTMLIngestor
) -> Optional[Dict[str, Any]]:
    print(f"[SENTENCES PROCESSING TASK] Processing {html_file_path.name}...")
    try:
        processed_data = sentence_ingestor_chunked.ingest_file(html_file_path)
        if not processed_data:
            print(
                f"[SENTENCES PROCESSING TASK] Failed to ingest {html_file_path.name}"
            )
            return None

        json_file_name = f"{processed_data['id']}.json"
        json_file_path = SENTENCES_JSON_OUTPUT_DIR / json_file_name
        SENTENCES_JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        print(
            f"[SENTENCES PROCESSING TASK] Successfully processed and saved JSON for {html_file_path.name}"
        )
        return processed_data

    except Exception as e:
        print(
            f"[SENTENCES PROCESSING TASK] Error processing {html_file_path.name}: {e}"
        )
        return None


async def send_sentence_to_api(sentence_data: Dict[str, Any]) -> bool:
    api_url = API_URL

    headers = {"Content-Type": "application/json"}
    # if api_key:
    #     headers["Authorization"] = f"Bearer {api_key}"

    sentence_id = sentence_data.get("id")
    print(
        f"[SENTENCES PROCESSING TASK] Attempting to send sentence {sentence_id} to API: {api_url}"
    )

    # Formatear datos para Rails API
    formatted_data = {
        "number": sentence_data.get("number"),
        "court": sentence_data.get("court"),
        "importance": sentence_data.get("importance"),
        "sentence_type": sentence_data.get("sentence_type"),
        "date": sentence_data.get("date"),
        "file_number": sentence_data.get("file_number"),
        "procedure": sentence_data.get("procedure"),
        "subjects": sentence_data.get("subjects", []),
        "summary": sentence_data.get("summary"),
        "text": sentence_data.get("text"),
        "raw_text": sentence_data.get("raw_text"),
        "signatories": sentence_data.get("signatories", []),
        "discordants": sentence_data.get("discordants", []),
        "editors": sentence_data.get("editors", []),
        "descriptors": sentence_data.get("descriptors", []),
        "short_embeddings_attributes": sentence_data.get("short_embeddings_attributes", []),
        "long_embeddings_attributes": sentence_data.get("long_embeddings_attributes", [])
    }
    payload = {"sentence": formatted_data}

    try:
        response = requests.post(api_url, json=payload, headers=headers)

        if response.ok:
            print(
                f"[SENTENCES PROCESSING TASK] Successfully sent sentence {sentence_id} to API. Status: {response.status_code}"
            )
            return True
        else:
            print(
                f"[SENTENCES PROCESSING TASK] API error for sentence {sentence_id} when sending to {api_url}: {response.status_code} - {response.text}"
            )
            return False
    except Exception as e:
        print(
            f"[SENTENCES PROCESSING TASK] Unexpected error sending sentence {sentence_id} to API {api_url}: {e}"
        )
    return False


async def run_sentence_processing_job(
    processing_start_date: datetime, processing_end_date: datetime
):
    print("[SENTENCES PROCESSING TASK] --- Starting sentence processing job ---")
    print(
        f"[SENTENCES PROCESSING TASK] Processing for period: {processing_start_date.date()} to {processing_end_date.date()}"
    )

    SENTENCES_HTML_BASE_DIR.mkdir(parents=True, exist_ok=True)
    SENTENCES_JSON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_SENTENCES_DIR.mkdir(parents=True, exist_ok=True)

    print("[SENTENCES PROCESSING TASK] --- Step 1: Scraping sentences ---")
    scraped_files = await scrape_sentences_for_period(
        start_date=processing_start_date,
        end_date=processing_end_date,
        output_base_dir=SENTENCES_HTML_BASE_DIR,
    )

    if not scraped_files:
        print(
            "[SENTENCES PROCESSING TASK] No HTML files found in the scraping directory. Terminating job for this period."
        )
        return

    print(
        "[SENTENCES PROCESSING TASK] --- Step 2: Ingesting, Processing HTML files, and Sending to API ---"
    )

    chunking_params_for_ingestor = {
        "max_tokens": MAX_TOKENS_CHUNKING,
        "overlap": OVERLAP,
    }
    sentence_ingestor_chunked = SentenceHTMLIngestor(
        chunking_params=chunking_params_for_ingestor, force_chunking=True
    )

    success_count = 0
    failure_count = 0

    for html_file_path in scraped_files:
        try:
            processed_data = process_single_sentence_html(
                html_file_path, sentence_ingestor_chunked
            )

            if processed_data:
                api_send_success = await send_sentence_to_api(processed_data)

                if api_send_success:
                    try:
                        html_file_path.unlink()
                        print(
                            f"[SENTENCES PROCESSING TASK] Successfully processed, JSON saved, HTML file '{html_file_path.name}' deleted, and data sent to API."
                        )
                        success_count += 1
                    except OSError as e_unlink:
                        print(
                            f"[SENTENCES PROCESSING TASK] Data for '{html_file_path.name}' sent to API, but FAILED TO DELETE local HTML file: {e_unlink}. File remains in source directory."
                        )
                        failure_count += 1
                else:
                    print(
                        f"[SENTENCES PROCESSING TASK] Local processing of '{html_file_path.name}' successful (JSON saved), but failed to send to API. HTML file remains in source directory."
                    )
                    failure_count += 1
            else:
                print(
                    f"[SENTENCES PROCESSING TASK] Local processing failed for '{html_file_path.name}'. HTML file remains in source directory."
                )
                failure_count += 1

        except Exception as e:
            print(
                f"[SENTENCES PROCESSING TASK] Unexpected error in processing loop for file '{html_file_path.name}': {e}"
            )
            failure_count += 1

    print("[SENTENCES PROCESSING TASK] Sentence processing job finished")
    print(
        f"[SENTENCES PROCESSING TASK] Summary: {success_count} files processed successfully, {failure_count} files failed."
    )
