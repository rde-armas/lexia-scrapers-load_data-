from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright


IMPO_BASE_URL = "https://www.impo.com.uy/"
IMPO_SEARCH_URL = "https://www.impo.com.uy/cgi-bin/bases/consultaBasesBS.cgi?tipoServicio=3&realizarconsulta=SI&idconsulta={}&nrodocdesdehasta={}-{}"


async def scrape_norms(start_date: datetime, end_date: datetime, type: int):
    try:
        import os
        print("[IMPO SCRAPER] Scraping norms for period: {} to {}".format(start_date.strftime("%d/%m/%Y"), end_date.strftime("%d/%m/%Y")))
        base_data_path = Path(os.getenv("LEXIA_BRAIN_DATA_PATH", "./data"))
        path_file = base_data_path / "norms_links.txt"
        path_file.parent.mkdir(parents=True, exist_ok=True)
        file = open(path_file, "a")

        async with async_playwright() as p:
            norm_count = 0
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto(IMPO_BASE_URL)
            await page.wait_for_load_state()
            await page.click('text="Banco de Datos de IMPO"')
            await page.wait_for_load_state()
            await page.select_option('[id="combo1"]', f"{type}")

            # Format dates to DD/MM/YYYY
            formatted_start_date = start_date.strftime("%d/%m/%Y")
            formatted_end_date = end_date.strftime("%d/%m/%Y")

            await page.evaluate(
                f'document.querySelector("input[name=fechadiar1]").value = "{formatted_start_date}"'
            )
            await page.evaluate(
                f'document.querySelector("input[name=fechadiar2]").value = "{formatted_end_date}"'
            )

            # Launch search
            await page.click('[id="botonBuscar"]')
            await page.wait_for_load_state()

            try:
                await page.wait_for_selector(
                    ".table.table-hover tbody > tr", timeout=60000, state="visible"
                )
            except Exception as e:
                print(
                    f"[IMPO SCRAPER] No results found or table loading timeout: {e}"
                )
                file.close()
                return
            prev_count = 0

            while True:
                rows = await page.query_selector_all(".table.table-hover tbody > tr")
                curr_count = len(rows)
                if curr_count == prev_count:
                    break
                prev_count = curr_count

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(10000)
            rows = await page.query_selector_all(".table.table-hover tbody > tr")
            for row in rows:
                impo_link_cell = await row.query_selector("td:nth-child(2) > a")
                impo_link_cell_text = await impo_link_cell.inner_text()

                if "Documento actualizado" in impo_link_cell_text:
                    impo_link = await impo_link_cell.get_attribute("href")

                    if impo_link:
                        norm_count += 1
                        print(f"[IMPO SCRAPER] Norm {norm_count}: {impo_link}")
                        file.write(f"https://www.impo.com.uy/{impo_link}\n")
                    else:
                        print(
                            f"[IMPO SCRAPER] No link found for norm {norm_count}"
                        )

            print(f"[IMPO SCRAPER] Total norms scraped: {norm_count}")

            await context.close()
            await page.close()
            await browser.close()

        file.close()
    except Exception as e:
        print(f"[IMPO SCRAPER] An error occurred: {e}", exc_info=True)
