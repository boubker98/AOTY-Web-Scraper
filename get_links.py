# get_links_async.py
import asyncio
import aiohttp
import random
from bs4 import BeautifulSoup
from prefect import task

MAX_CONCURRENT = 5
REQUEST_DELAY = (0.5, 1.5)  # seconds


@task(retries=3, retry_delay_seconds=3, persist_result=False)
async def get_links(base_url, page_count):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

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
                return results

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [fetch_page(session, page) for page in range(1, page_count + 1)]
        pages_data = await asyncio.gather(*tasks)
        # Flatten the list of lists
        return [album for page in pages_data for album in page]
