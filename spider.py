import requests
from requests_html import AsyncHTMLSession
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor, as_completed
import logger
from asyncio import sleep
from urllib.parse import urljoin, urlparse
from collections import namedtuple
from models import Page
session = None
USER_AGENT = "spider@monkefun"
BASE_HEADERS = headers = {"User-Agent": USER_AGENT}


def normalize_url(url: str):
    return url.rstrip("/")


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
    PageLinksData = namedtuple("PageLinksData", ["status", "links"])
    logger.debug(f"Getting {source_url}")
    sitesession = await session.get(source_url, headers=BASE_HEADERS)
    siteheaders = sitesession.headers
    content_type = siteheaders["content-type"]
    if "text/html" not in content_type:
        logger.debug(f"Page {source_url} is not an html page, skipping..")
        return None
    await sitesession.html.arender(timeout=60)
    normal_links = [normalize_url(link)
                    for link in sitesession.html.absolute_links]
    return Page(url=source_url, status=sitesession.status_code, links=normal_links)


async def crawl_page_links(source_url: str, session: AsyncHTMLSession):
    treated = []
    source_parsed = urlparse(source_url)
    response = await get_page_links(source_url, session)
    links = response.links
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

CrawledPage = namedtuple("CrawledPage", ["pageUrl", "status", "links"])
class WebCrawler:
    def __init__(self, website: str, limit: int, base_interval: float):
        self._website_url = normalize_url(website)
        self._robots_path = urljoin(self._website_url, "/robots.txt")
        self._queue = [self._robots_path, self._website_url]
        self._handled = []
        self._counter = 0
        self._limit = limit
        self._interval = base_interval
        self._web_session = AsyncHTMLSession()
        self._robots_data = None

    async def load_more_links(self, from_page: str):
        page = await get_page_links(from_page, self._web_session)
        if page is None:
            return None
        links = page.links
        queued_urls = self._queue
        new_links = [
            link for link in links if link not in queued_urls + self._handled and urlparse(link).netloc == page.domain]
        self._queue.extend(new_links)
        return page

    def __aiter__(self):
        return self

    def load_robots_txt(self):
        parser = RobotFileParser(self._robots_path)
        parser.read()
        self._robots_data = parser

    @property
    def crawl_delay(self):
        if self._robots_data is None:
            return self._interval
        return self._interval + (self._robots_data.crawl_delay(USER_AGENT) or 0)

    def crawl_allowed(self, page: str):
        return self._robots_data.can_fetch(USER_AGENT, page)

    async def __anext__(self):
        if len(self._queue) == 0 or self._counter == self._limit:
            raise StopAsyncIteration

        curr = self._queue.pop(0)

        if self._robots_data is not None and not self.crawl_allowed(curr):
            logger.info(
                f"{USER_AGENT} is not allowed to fetch {curr}, not loading more links from here")
            self._handled.append(curr)
            return Page(url=curr, links=[], status=0)

        if self._counter > 0 and self.crawl_delay > 0:
            logger.info(f"Waiting... {str(self.crawl_delay)}s")
            await sleep(self.crawl_delay)

        if curr == self._robots_path:
            self.load_robots_txt()
            return Page(url=curr, status=200, links=[])

        self._counter += 1
        self._handled.append(curr)
        try:
            page = await self.load_more_links(curr)
            return page
        except Exception as e:
            logger.error(
                f"Failed to proccess page links for page {curr}, error: {repr(e)}")
            return None
