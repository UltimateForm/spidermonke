from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass(frozen=True, kw_only=True)
class Page:
    url: str
    links: list[str]
    status: int
    local_links: list[str] = field(init=False)
    path: str = field(init=False)
    domain: str = field(init=False)
    scheme: str = field(init=False)
    params: str = field(init=False)

    def __post_init__(self):
        parsed = urlparse(self.url)
        object.__setattr__(self, "path", parsed.path if len(parsed.path) > 0 else "/")
        object.__setattr__(self, "domain", parsed.netloc)
        object.__setattr__(self, "scheme", parsed.scheme)
        object.__setattr__(self, "params", parsed.params)
        local_links = [link for link in self.links if urlparse(
            link).netloc == self.domain]
        object.__setattr__(self, "local_links", local_links)

