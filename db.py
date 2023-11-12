import logger
from neo4j import AsyncGraphDatabase, ManagedTransaction, AsyncManagedTransaction, Result
from os import environ
from models import Page
import asyncio

DB_URI = environ["NEO4J_URI"]
DB_USER = environ["NEO4J_USERNAME"]
DB_PASSWORD = environ["NEO4J_PASSWORD"]


class DBClient:
    def __init__(self):
        self._driver = AsyncGraphDatabase.driver(
            DB_URI, auth=(DB_USER, DB_PASSWORD))

    async def close(self):
        await self._driver.close()

    async def put_pages(self, pages: list):
        async with self._driver.session(database="neo4j") as session:
            await session.execute_write(self._put_pages, pages)
            for page in filter(lambda p : len(p.local_links) > 0, pages):
                await session.execute_write(self._put_rels, page)

    @staticmethod
    async def _put_pages(tx: AsyncManagedTransaction, pages: list[Page]):
        result = await tx.run("""
                                WITH $pages as pages
                                UNWIND pages as item
                                MERGE (p:PAGE {url:item.url})
                                ON CREATE
                                    SET p.path=item.path
                                    SET p.url=item.url
                                    SET p.domain=item.domain
                                    SET p.scheme=item.scheme
                                    SET p.status=item.status
                                    SET p.params=item.params
                                RETURN p
                            """, pages=[page.__dict__ for page in pages])
        data = await result.data()
        return data

    @staticmethod
    async def _put_rels(tx: AsyncManagedTransaction, page: Page):
        result = await tx.run("""
                                UNWIND $page.local_links as link
                                MATCH (parent:PAGE {url:$page.url}),(child:PAGE {url:link})
                                CREATE (parent)-[:LINKS]->(child)
                            """, page=page.__dict__)
        return result


async def main():
    greeter = DBClient()
    await greeter.put_pages([Page(url="https://books.toscrape.com/", status=200, links=["https://books.toscrape.com/path"]),
                             Page(url="https://books.toscrape.com/path", status=200,
                                  links=["https://books.toscrape.com/path/child"]),
                             Page(url="https://books.toscrape.com/path/child", status=200, links=[])])
    await greeter.close()
if __name__ == "__main__":
    asyncio.run(main())
