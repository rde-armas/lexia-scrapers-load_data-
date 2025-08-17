import asyncio
import random
import re
from datetime import timedelta
from pathlib import Path

from playwright.async_api import async_playwright, expect


BJN_URL = "https://bjn.poderjudicial.gub.uy/BJNPUBLICA/busquedaSelectiva.seam"
MAX_RETRIES = 5


def generate_filename(sentence_date, sentence_number, sentence_court, sentence_type):
    formatted_sentence_date = sentence_date.replace("/", "_")
    formatted_sentence_number = sentence_number.replace("/", "_")
    formatted_sentence_court = sentence_court.replace(" ", "_").replace("º", "").upper()
    formatted_sentence_type = sentence_type.upper()

    return (
        formatted_sentence_number
        + "_"
        + formatted_sentence_court
        + "_"
        + formatted_sentence_type
        + "_"
        + formatted_sentence_date
        + ".html"
    )


async def scrape_sentences(
    session_start_date, session_end_date, output_dir: Path, retry_count=0
):
    try:

        await asyncio.sleep(random.uniform(10, 30))

        async with async_playwright() as p:
            task_firm = f"[{session_start_date.strftime('%d/%m/%Y')} - {session_end_date.strftime('%d/%m/%Y')}]"

            browser = await p.chromium.launch(headless=True, slow_mo=100)

            start_date = session_start_date

            while start_date < session_end_date:
                end_date = min(start_date + timedelta(days=6), session_end_date)

                context = await browser.new_context()
                page = await context.new_page()

                goto_success = False
                max_goto_retries = 3
                goto_retry_count = 0

                while not goto_success and goto_retry_count < max_goto_retries:
                    try:
                        print(
                            f"[SENTENCES SCRAPING] {task_firm} Navigating to BJN website. Attempt {goto_retry_count + 1}"
                        )
                        await page.goto(
                            BJN_URL, timeout=45000
                        )  # Increase timeout to 45 seconds
                        goto_success = True
                    except Exception as e:
                        goto_retry_count += 1
                        if goto_retry_count >= max_goto_retries:
                            print(
                                f"[SENTENCES SCRAPING] {task_firm} Failed to navigate to BJN website after {max_goto_retries} attempts: {e}"
                            )
                            raise
                        backoff_seconds = 5 * (
                            2**goto_retry_count
                        )  # Exponential backoff
                        print(
                            f"[SENTENCES SCRAPING] {task_firm} Navigation failed: {e}. Retrying in {backoff_seconds} seconds..."
                        )
                        await asyncio.sleep(backoff_seconds)

                print(
                    f"[SENTENCES SCRAPING] {task_firm} Scraping period from {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}"
                )

                await page.fill(
                    '[id="formBusqueda:j_id20:j_id23:fechaDesdeCalInputDate"]',
                    start_date.strftime("%d/%m/%Y"),
                )
                await page.fill(
                    '[id="formBusqueda:j_id20:j_id147:fechaHastaCalInputDate"]',
                    end_date.strftime("%d/%m/%Y"),
                )

                await page.evaluate(
                    """
                        total_per_page_input = document.querySelector('[id="formBusqueda:j_id20:j_id223:cantPagcomboboxValue"]');
                        total_per_page_input.value = 50;
                    """
                )

                await page.select_option(
                    '[name="formBusqueda:j_id20:j_id240:j_id248"]', "FECHA_ASCENDENTE"
                )

                await page.click('[id="formBusqueda:j_id20:Search"]')

                try:
                    is_hidden = not await page.locator(
                        '[id="_viewRoot:status.start"]'
                    ).is_visible()

                    if not is_hidden:
                        print(
                            f"[SENTENCES SCRAPING] {task_firm} Status element is visible, waiting for it to be hidden."
                        )
                        await expect(
                            page.locator('[id="_viewRoot:status.start"]')
                        ).to_be_hidden(timeout=15000)
                    else:
                        print(
                            f"[SENTENCES SCRAPING] {task_firm} Status element is already hidden."
                        )

                    await asyncio.sleep(1)
                    await asyncio.sleep(1)
                except Exception as e:
                    print(
                        f"[SENTENCES SCRAPING] {task_firm} Error while checking status element: {e}"
                    )

                no_results = await page.locator(
                    '[id="formBusqueda:errores"]'
                ).is_visible()

                if no_results:
                    print(
                        f"[SENTENCES SCRAPING] {task_firm} No results found for period from {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}."
                    )

                    start_date = end_date

                    continue

                # Handle pagination
                paginator = await page.query_selector(
                    '[id="formResultados:zonaPaginador"]'
                )
                paginator_text_element = await paginator.query_selector(
                    "span:nth-of-type(2)"
                )
                paginator_text = await paginator_text_element.inner_text()

                # Extract total pages from paginator text
                pattern = r"Página\s+(\d+)\s+de\s+(\d+)"
                match = re.search(pattern, paginator_text)

                current_page = 1
                total_pages = int(match.group(2))

                # Loop through pages
                while current_page <= total_pages:
                    print(
                        f"[SENTENCES SCRAPING] {task_firm} Scraping page {current_page} of {total_pages}."
                    )

                    await page.wait_for_selector(
                        '[id="formResultados:dataTable"]', timeout=3000
                    )
                    rows = await page.query_selector_all(
                        '[id="formResultados:dataTable"] > tbody > tr'
                    )

                    current_row = 1
                    total_rows = len(rows)

                    # Loop through rows
                    while current_row <= total_rows:
                        print(
                            f"[SENTENCES SCRAPING] {task_firm} Scraping sentence {current_row} of {total_rows}."
                        )

                        row = rows[current_row - 1]

                        sentence_date_cell = await row.query_selector("td:nth-child(1)")
                        sentence_type_cell = await row.query_selector("td:nth-child(2)")
                        sentence_number_cell = await row.query_selector(
                            "td:nth-child(3)"
                        )
                        sentence_court_cell = await row.query_selector(
                            "td:nth-child(4)"
                        )

                        sentence_date = await sentence_date_cell.inner_text()
                        sentence_type = await sentence_type_cell.inner_text()
                        sentence_number = await sentence_number_cell.inner_text()
                        sentence_court = await sentence_court_cell.inner_text()

                        file_name = generate_filename(
                            sentence_date,
                            sentence_number,
                            sentence_court,
                            sentence_type,
                        )

                        # Improved popup handling with retry logic and error handling
                        max_popup_retries = 3
                        popup_retry_count = 0

                        while popup_retry_count < max_popup_retries:
                            try:

                                async with context.expect_page(
                                    timeout=15000
                                ) as sentence_popup:
                                    await sentence_number_cell.click(modifiers=["Alt"])

                                    await asyncio.sleep(0.5)

                                sentence_page = await sentence_popup.value

                                await sentence_page.wait_for_load_state(
                                    "domcontentloaded", timeout=10000
                                )

                                try:
                                    await sentence_page.wait_for_selector(
                                        "body", timeout=5000
                                    )

                                    sentence_html = await sentence_page.inner_html(
                                        "body"
                                    )

                                    output_dir.mkdir(parents=True, exist_ok=True)

                                    with open(
                                        output_dir / file_name, "w+", encoding="utf-8"
                                    ) as file:
                                        file.write(sentence_html)

                                    await sentence_page.close()

                                    break

                                except Exception as page_error:
                                    print(
                                        f"[SENTENCES SCRAPING] {task_firm} Error processing popup page: {page_error}"
                                    )
                                    if sentence_page:
                                        await sentence_page.close()
                                    raise

                            except Exception as popup_error:
                                popup_retry_count += 1
                                print(
                                    f"[SENTENCES SCRAPING] {task_firm} Failed to open popup for sentence {sentence_number}, attempt {popup_retry_count}: {popup_error}"
                                )

                                if popup_retry_count >= max_popup_retries:
                                    print(
                                        f"[SENTENCES SCRAPING] {task_firm} Failed to open popup after {max_popup_retries} attempts, skipping sentence {sentence_number}"
                                    )
                                    break

                                backoff_seconds = 2**popup_retry_count
                                await asyncio.sleep(backoff_seconds)
                        await page.bring_to_front()

                        current_row += 1

                        rows = await page.query_selector_all(
                            '[id="formResultados:dataTable"] > tbody > tr'
                        )

                    await page.click('[id="formResultados:sigLink"]')

                    if current_page != total_pages:
                        try:
                            await asyncio.sleep(1)

                            try:
                                await expect(
                                    page.locator('[id="_viewRoot:status.start"]')
                                ).to_be_visible(timeout=2000)
                                await expect(
                                    page.locator('[id="_viewRoot:status.start"]')
                                ).to_be_hidden(timeout=5000)
                            except Exception as loading_error:
                                print(
                                    f"[SENTENCES SCRAPING] Loading indicator not detected, using alternative wait method: {loading_error}"
                                )
                                await page.wait_for_load_state(
                                    "networkidle", timeout=10000
                                )

                            await page.wait_for_selector(
                                '[id="formResultados:dataTable"]',
                                state="visible",
                                timeout=10000,
                            )
                        except Exception as nav_error:
                            print(
                                f"[SENTENCES SCRAPING] Navigation error: {nav_error}"
                            )

                    current_page += 1

                    retry_count = 0

                start_date = end_date

                await context.close()
                await page.close()

            await browser.close()
    except Exception as e:
        task_firm = f"[{session_start_date.strftime('%d/%m/%Y')} - {session_end_date.strftime('%d/%m/%Y')}]"

        print(
            f"[SENTENCES SCRAPING] {task_firm} Error encountered: {e}", exc_info=True
        )

        if retry_count < MAX_RETRIES:
            print(
                f"[SENTENCES SCRAPING] {task_firm} Retrying period from {session_start_date.strftime('%d/%m/%Y')} to {session_end_date.strftime('%d/%m/%Y')}. Attempt {retry_count + 1}."
            )

            await scrape_sentences(
                session_start_date, session_end_date, output_dir, retry_count + 1
            )
        else:
            print(
                f"[SENTENCES SCRAPING] {task_firm} Max retries reached for period from {session_start_date.strftime('%d/%m/%Y')} to {session_end_date.strftime('%d/%m/%Y')}. Moving to next period."
            )
