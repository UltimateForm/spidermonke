import requests
from requests_html import AsyncHTMLSession
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor, as_completed
import logger
from typing import Awaitable
from asyncio import sleep
from urllib.parse import urljoin

session = None
USER_AGENT = "spider@monkefun"
BASE_HEADERS = headers = {"User-Agent": USER_AGENT}


def load_page_link_data(link: str, source_host: str, source_url: str):
    logger.info(f"Processing {link}...")
    link_parsed = urlparse(link)
    link_data = {"referer": source_url, "url": link, "scheme": link_parsed.scheme,
                 "host": link_parsed.netloc, "path": link_parsed.path}
    link_data["is_local"] = int(link_parsed.netloc == source_host)
    if link_parsed.scheme == "tel":
        link_data["status_code"] = 0
        logger.info(
            f"Link {link} is a tel: link therefore not checking status")
        return link_data
    req = requests.get(link, headers=BASE_HEADERS)
    link_data["status_code"] = req.status_code
    return link_data


async def get_page_links(source_url: str, session: AsyncHTMLSession):
    logger.debug(f"Getting {source_url}")
    sitesession = await session.get(source_url, headers=BASE_HEADERS)
    siteheaders = sitesession.headers
    content_type = siteheaders["content-type"]
    if "text/html" not in content_type:
        logger.debug(f"Page {source_url} is not an html page, skipping..")
        return []
    logger.info(f"Rendering {source_url}")
    await sitesession.html.arender(timeout=60)
    return sitesession.html.absolute_links


async def crawl_page_links(source_url: str, session: AsyncHTMLSession):
    treated = []
    source_parsed = urlparse(source_url)
    links = await get_page_links(source_url, session)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(
            load_page_link_data, link, source_parsed.netloc, source_url): link for link in links}
        for future in as_completed(futures):
            target_url = futures[future]
            try:
                data = future.result()
                treated.append(data)
            except:
                logger.error(f"Failed processing for {target_url}")
    return treated


class WebCrawler:
    def __init__(self, website: str, limit: int, base_interval: float):
        self.__website_url = website
        self.__robots_path = urljoin(self.__website_url, "/robots.txt")
        self.__queue = [self.__robots_path, self.__website_url]
        self.__handled = []
        self.__counter = 0
        self.__limit = limit
        self.__interval = base_interval
        self.__web_session = AsyncHTMLSession()
        self.__robots_data = None

    async def load_more_links(self, from_page: str):
        links = await get_page_links(from_page, self.__web_session)
        new_links = [
            link for link in links if link not in self.__queue + self.__handled]
        self.__queue.extend(new_links)

    def __aiter__(self):
        return self

    def load_robots_txt(self):
        parser = RobotFileParser(self.__robots_path)
        parser.read()
        self.__robots_data = parser

    @property
    def crawl_delay(self):
        if self.__robots_data is None:
            return self.__interval
        return self.__interval + (self.__robots_data.crawl_delay(USER_AGENT) or 0)

    async def __anext__(self):
        if len(self.__queue) == 0 or self.__counter == self.__limit:
            raise StopAsyncIteration

        curr = self.__queue.pop(0)

        if self.__robots_data is not None and not self.__robots_data.can_fetch(USER_AGENT, curr):
            logger.info(
                f"{USER_AGENT} is not allowed to fetch {curr}, not loading more links from here")
            self.__handled.append(curr)
            return curr

        if self.__counter > 0 and self.crawl_delay > 0:
            logger.info(f"Waiting... {str(self.crawl_delay)}s")
            await sleep(self.crawl_delay)

        if curr == self.__robots_path:
            self.load_robots_txt()
            return curr

        self.__counter += 1
        self.__handled.append(curr)
        try:
            await self.load_more_links(curr)
            return curr
        except:
            logger.error(f"Failed to proccess page links for page {curr}")
            return None
