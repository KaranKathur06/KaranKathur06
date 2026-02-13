import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import requests


@dataclass
class RateLimit:
    remaining: Optional[int]
    reset_epoch: Optional[int]


class GitHubClient:
    def __init__(self, token: str, base_url: str = "https://api.github.com") -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "github-analytics-engine",
            }
        )

    def _parse_rate_limit(self, headers: Dict[str, str]) -> RateLimit:
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        return RateLimit(
            remaining=int(remaining) if remaining and remaining.isdigit() else None,
            reset_epoch=int(reset) if reset and reset.isdigit() else None,
        )

    def _maybe_sleep_for_rate_limit(self, rl: RateLimit, threshold: int = 1) -> None:
        if rl.remaining is None or rl.reset_epoch is None:
            return
        if rl.remaining > threshold:
            return

        now = int(time.time())
        sleep_s = max(0, rl.reset_epoch - now) + 2
        time.sleep(min(sleep_s, 60 * 5))

    def request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        resp = self.session.request(method=method, url=url, params=params, timeout=30)
        rl = self._parse_rate_limit(resp.headers)

        if resp.status_code == 403 and rl.remaining == 0:
            self._maybe_sleep_for_rate_limit(rl, threshold=0)
            resp = self.session.request(method=method, url=url, params=params, timeout=30)

        resp.raise_for_status()
        self._maybe_sleep_for_rate_limit(rl)
        return resp.json()

    def paginate(
        self, path: str, params: Optional[Dict[str, Any]] = None, per_page: int = 100
    ) -> Iterator[List[Dict[str, Any]]]:
        page = 1
        while True:
            merged = dict(params or {})
            merged.update({"per_page": per_page, "page": page})
            data = self.request("GET", path, params=merged)
            if not isinstance(data, list) or len(data) == 0:
                break
            yield data
            if len(data) < per_page:
                break
            page += 1

    def get_authenticated_user(self) -> Dict[str, Any]:
        return self.request("GET", "/user")

    def list_user_repos_public(self, username: str) -> Iterator[List[Dict[str, Any]]]:
        return self.paginate(f"/users/{username}/repos", params={"type": "owner", "sort": "updated"})

    def list_user_repos_auth(self) -> Iterator[List[Dict[str, Any]]]:
        return self.paginate(
            "/user/repos",
            params={"affiliation": "owner", "visibility": "all", "sort": "updated"},
        )

    def list_commits(self, owner: str, repo: str, author: str) -> Iterator[List[Dict[str, Any]]]:
        return self.paginate(
            f"/repos/{owner}/{repo}/commits",
            params={"author": author},
        )

    def get_languages(self, owner: str, repo: str) -> Dict[str, int]:
        data = self.request("GET", f"/repos/{owner}/{repo}/languages")
        if not isinstance(data, dict):
            return {}
        out: Dict[str, int] = {}
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
        return out
