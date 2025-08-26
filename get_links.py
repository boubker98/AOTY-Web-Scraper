# get_links_async.py
import asyncio
import aiohttp
import random
from bs4 import BeautifulSoup
from prefect import task, get_run_logger

MAX_CONCURRENT = 5
REQUEST_DELAY = (0.5, 1.5)  # seconds


@task(retries=3, retry_delay_seconds=3, persist_result=False)
async def get_links(base_url):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    logger = get_run_logger()

    async def fetch_page(session, page):
        async with semaphore:
            await asyncio.sleep(random.uniform(*REQUEST_DELAY))
            async with session.get(f"{base_url}/{page}") as resp:
                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                albums_container = soup.find_all("div", class_="albumListRow")

                results = []
                for album in albums_container:
                    title_tag = album.find("h2", class_="albumListTitle").find(
                        "a", itemprop="url"
                    )
                    title = title_tag.get_text(strip=True) if title_tag else None
                    link = (
                        "https://www.albumoftheyear.org" + title_tag["href"]
                        if title_tag
                        else None
                    )
                    results.append({"title": title, "link": link})

                return results, soup

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        # --- Step 1: Fetch first page to detect pagination ---
        first_results, soup = await fetch_page(session, 1)

        pagination = soup.find_all("div", class_="pageSelectSmall")
        if pagination:
            try:
                page_count = int(pagination[-1].get_text(strip=True).split()[-1])
            except Exception:
                page_count = 1
        else:
            page_count = 1

        logger.info(f"Total pages to be scraped: {page_count}")

        # --- Step 2: Fetch the remaining pages in parallel ---
        tasks = [fetch_page(session, page) for page in range(2, page_count + 1)]
        other_pages_data = await asyncio.gather(*tasks) if tasks else []

        # Flatten results
        all_results = first_results + [
            res for page_data, _ in other_pages_data for res in page_data
        ]
        return all_results
