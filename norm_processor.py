import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests

from scraper import impo_scraper
from norms_json_ingestor import NormsJsonIngestor

# Configuración del API
NORMS_API_URL = "http://api.lvh.me:3000/v1/norms"

BASE_DATA_PATH = Path(os.getenv("LEXIA_BRAIN_DATA_PATH", "./data"))

NORMS_JSON_DIR = BASE_DATA_PATH / "norms" / "json"
PROCESSED_JSON_DIR = BASE_DATA_PATH / "norms" / "processed_json"
NORMS_LINKS_FILE_PATH = BASE_DATA_PATH / "norms_links.txt"

SCRAPER_TASK_CONFIG = {
    "international": 10,
}

API_NORMTYPE_IDS_RAILS = {
    "international": 4,
}



def send_norm_to_api(norm_payload_with_key: Dict[str, Any]) -> bool:
    norm_data_for_logging = norm_payload_with_key.get("norm", {})
    norm_identifier = norm_data_for_logging.get("number") or norm_data_for_logging.get(
        "title", "Unknown Norm"
    )

    print(
        f"[NORMS API TASK] Sending norm {norm_identifier} to API: {NORMS_API_URL}"
    )

    try:
        response = requests.post(NORMS_API_URL, json=norm_payload_with_key)
        response.raise_for_status()
        print(
            f"[NORMS API TASK] Successfully sent norm {norm_identifier}. Status: {response.status_code}"
        )
        return True
    except Exception as e:
        print(
            f"[NORMS API TASK] Unexpected error sending norm {norm_identifier}: {e}"
        )
        return False


async def run_scraper_for_type(
    start_date: datetime, end_date: datetime, norm_type: str, scraper_id: int
) -> List[str]:
    print(
        f"[NORMS SCRAPER TASK] Running scraper for norm type: {norm_type} (ID: {scraper_id})"
    )

    if NORMS_LINKS_FILE_PATH.exists():
        NORMS_LINKS_FILE_PATH.unlink()

    try:
        await impo_scraper.scrape_norms(start_date, end_date, scraper_id)
        print(f"[NORMS SCRAPER TASK] Completed scraping for {norm_type}")
    except Exception as e:
        print(
            f"[NORMS SCRAPER TASK] Error scraping {norm_type}: {e}"
        )
        return []

    if not NORMS_LINKS_FILE_PATH.exists():
        print(
            f"[NORMS SCRAPER TASK] No links file generated after scraping: {NORMS_LINKS_FILE_PATH}"
        )
        return []

    with open(NORMS_LINKS_FILE_PATH, "r", encoding="utf-8") as f:
        scraped_urls = [line.strip() for line in f if line.strip()]

    print(
        f"[NORMS SCRAPER TASK] Collected {len(scraped_urls)} URLs for {norm_type} from IMPO scraper"
    )
    return scraped_urls


async def run_norm_processing_job(start_date: datetime, end_date: datetime):
    print(
        f"[NORMS PROCESSING TASK] --- Starting norm processing job for period: {start_date.date()} to {end_date.date()} ---"
    )

    NORMS_JSON_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_JSON_DIR.mkdir(parents=True, exist_ok=True)

    norms_ingestor = NormsJsonIngestor()

    for norm_type, scraper_id in SCRAPER_TASK_CONFIG.items():
        print(
            f"[NORMS PROCESSING TASK] Starting full process for norm type: {norm_type}"
        )
        try:
            scraped_urls = await run_scraper_for_type(
                start_date, end_date, norm_type, scraper_id
            )
            if not scraped_urls:
                print(
                    f"[NORMS PROCESSING TASK] No URLs were scraped for {norm_type}. Continuing with next type."
                )
                continue
        except Exception as e:
            print(
                f"[NORMS PROCESSING TASK] Error running scraper for {norm_type}: {e}"
            )
            continue

        if not scraped_urls:
            print(
                f"[NORMS PROCESSING TASK] No URLs to process for {norm_type}."
            )
            continue

        print(
            f"[NORMS PROCESSING TASK] Found {len(scraped_urls)} URLs to process for {norm_type}."
        )

        processed_urls_count = 0
        failed_urls_details = []
        temp_json_path = None

        for url_index, norm_url in enumerate(scraped_urls):
            print(
                f"[NORMS PROCESSING TASK] Processing URL {url_index + 1}/{len(scraped_urls)}: {norm_url}"
            )
            temp_json_path = None

            try:
                json_fetch_url = (
                    norm_url
                    if "?json=true" in norm_url
                    else norm_url.rstrip("/") + "?json=true"
                )
                print(
                    f"[NORMS PROCESSING TASK] Fetching JSON from: {json_fetch_url}"
                )
                response = requests.get(json_fetch_url)
                response.raise_for_status()
                norm_json_content = response.json()

                norm_id = norm_json_content.get("nroNorma", "unknownID")
                url_id = norm_url.rstrip("/").split("/")[-1]
                temp_filename = f"{norm_type}_{norm_id}_{url_id}.json"

                norm_type_dir = NORMS_BASE_DIR / norm_type
                norm_type_dir.mkdir(parents=True, exist_ok=True)
                temp_json_path = norm_type_dir / temp_filename

                with open(temp_json_path, "w", encoding="utf-8") as f:
                    json.dump(norm_json_content, f, indent=2, ensure_ascii=False)
                print(
                    f"[NORMS PROCESSING TASK] Temporarily saved JSON to {temp_json_path}"
                )

                norm_data = norms_ingestor.ingest_file(temp_json_path)
                if not norm_data:
                    print(
                        f"[NORMS PROCESSING TASK] Ingestor couldn't process the norm data from {temp_filename}"
                    )

                api_payload = norm_data.copy()
                api_payload["impo_url"] = norm_url

                api_payload["norm_type"] = API_NORMTYPE_IDS_RAILS.get(norm_type)
                if api_payload["norm_type"] is None:
                    print(
                        f"[NORMS PROCESSING TASK] No API norm_type ID found for internal type: {norm_type}. Skipping URL {norm_url}"
                    )
                    failed_urls_details.append(
                        (norm_url, f"No API norm_type ID for {norm_type}")
                    )
                    continue

                if "processed_articles" in api_payload:
                    api_payload["articles_attributes"] = api_payload.pop(
                        "processed_articles"
                    )
                else:
                    api_payload["articles_attributes"] = []

                api_payload.pop("norm_id", None)
                final_api_payload = {"norm": api_payload}

                if api_payload.get("articles_attributes"):
                    print(
                        f"[NORMS PROCESSING TASK] Sending norm with {len(api_payload.get('articles_attributes'))} articles to API"
                    )
                    api_send_success = send_norm_to_api(final_api_payload)

                    if api_send_success:
                        if temp_json_path.exists():
                            temp_json_path.unlink()
                        print(
                            "[NORMS PROCESSING TASK] Successfully processed, sent to API, and removed local copies"
                        )
                        processed_urls_count += 1
                    else:
                        print(
                            f"[NORMS PROCESSING TASK] API send failed for norm {norm_id}. Keeping local copy for retry."
                        )
                else:
                    print(
                        f"[NORMS PROCESSING TASK] Norm {norm_id} has no articles to send. Skipping API send."
                    )
                    if temp_json_path.exists():
                        temp_json_path.unlink()
                    processed_urls_count += 1

            except Exception as e:
                error_msg = f"Error processing {norm_url} (temp file: {temp_json_path if temp_json_path else 'N/A'}): {e}"
                print(f"[NORMS PROCESSING TASK] {error_msg}")
                failed_urls_details.append((norm_url, str(e)))

        if failed_urls_details:
            with open(NORMS_LINKS_FILE_PATH, "w", encoding="utf-8") as f_failed:
                for url, _ in failed_urls_details:
                    f_failed.write(url + "\n")
            print(
                f"[NORMS PROCESSING TASK] Updated {NORMS_LINKS_FILE_PATH} with {len(failed_urls_details)} failed URLs for retry of {norm_type}."
            )
        else:
            NORMS_LINKS_FILE_PATH.unlink(missing_ok=True)
            print(
                f"[NORMS PROCESSING TASK] All {len(scraped_urls)} URLs for {norm_type} processed successfully."
            )

        print(
            f"[NORMS PROCESSING TASK] --- Finished processing {norm_type} norms ---"
        )
        print(
            f"[NORMS PROCESSING TASK] {norm_type} Summary: {processed_urls_count} URLs processed successfully, {len(failed_urls_details)} failed."
        )

    print("[NORMS PROCESSING TASK] --- Complete norm processing job finished ---")
