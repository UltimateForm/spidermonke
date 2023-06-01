from csv import DictWriter
from spider import crawl_page_links, WebCrawler
import logger
import asyncio

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
    async for page in WebCrawler("http://quotes.toscrape.com", 5, 0):
        logger.info(f"On page {page}")
    return
    await web_iterator("http://quotes.toscrape.com", collect_links)
    if len(links_dict) == 0:
        return
    keys = links_dict[0].keys()
    return
    with open("links-data.csv", "w", newline="") as output_file:
        writer = DictWriter(output_file, keys)
        writer.writeheader()
        writer.writerows(links_dict)
        logger.info(f"File written at {output_file.name}")

if __name__ == "__main__":
    asyncio.run(main())
