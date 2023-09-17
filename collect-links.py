from csv import DictWriter
from spider import crawl_page_links, WebCrawler
import logger
import asyncio
import db
links_dict = []


async def collect_links(target_page: str):
    page_links = await crawl_page_links(target_page)
    links_dict.extend(page_links)
    local_pages = [link for link in page_links if link["is_local"]
                   == 1 and (link["scheme"] in ["http", "https"])]
    new_pages = [link["url"] for link in local_pages]
    return new_pages


async def main():
    logger.use_date_time_logger()
    logger.info("START")
    pages = []
    async for page in WebCrawler("http://quotes.toscrape.com", 20, 0):
        logger.info(f"On page {page.path}")
        pages.append(page)
    dbClient = db.DBClient()
    await dbClient.put_pages(pages)
    logger.info("END")

if __name__ == "__main__":
    asyncio.run(main())
