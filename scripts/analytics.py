import argparse
import logging
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pytz

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from github_client import GitHubClient
from readme_updater import ReadmeUpdater
from utils import ReadmeSection, format_kv_lines, format_lang_lines, to_percent


@dataclass(frozen=True)
class Config:
    username: str
    timezone: str
    include_private: bool
    exclude_forks: bool
    exclude_merge_commits: bool


TIME_BUCKETS: List[Tuple[str, int, int]] = [
    ("Morning", 6, 12),
    ("Day", 12, 18),
    ("Evening", 18, 22),
    ("Night", 22, 6),
]

WEEKDAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def parse_utc_timestamp(ts: str) -> datetime:
    # Example: 2024-01-01T10:11:12Z
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)


def bucket_time_of_day(local_dt: datetime) -> str:
    hour = local_dt.hour
    for name, start, end in TIME_BUCKETS:
        if start < end:
            if start <= hour < end:
                return name
        else:
            # Wrap-around bucket, e.g., Night 22 -> 6
            if hour >= start or hour < end:
                return name
    return "Night"


def is_merge_commit(commit_obj: Dict) -> bool:
    # REST commit objects for /commits include commit.message and sometimes parents list.
    message = (
        commit_obj.get("commit", {}).get("message", "")
        if isinstance(commit_obj, dict)
        else ""
    )
    if isinstance(message, str) and message.startswith("Merge "):
        return True
    parents = commit_obj.get("parents")
    if isinstance(parents, list) and len(parents) > 1:
        return True
    return False


def iter_repos(client: GitHubClient, cfg: Config) -> Iterable[Dict]:
    if cfg.include_private:
        for page in client.list_user_repos_auth():
            for repo in page:
                yield repo
        return

    for page in client.list_user_repos_public(cfg.username):
        for repo in page:
            yield repo


def aggregate(client: GitHubClient, cfg: Config) -> Tuple[int, Counter, Counter, Dict[str, int]]:
    tz = pytz.timezone(cfg.timezone)

    total_commits = 0
    tod = Counter({name: 0 for name, _, _ in TIME_BUCKETS})
    weekday = Counter({d: 0 for d in WEEKDAYS})
    lang_bytes: Dict[str, int] = defaultdict(int)

    seen_shas: Set[str] = set()

    for repo in iter_repos(client, cfg):
        if cfg.exclude_forks and repo.get("fork") is True:
            continue

        owner = repo.get("owner", {}).get("login")
        name = repo.get("name")
        if not owner or not name:
            continue

        # Languages
        try:
            langs = client.get_languages(owner, name)
            for lang, b in langs.items():
                lang_bytes[lang] += int(b)
        except Exception:
            logging.exception("Failed to fetch languages for %s/%s", owner, name)

        # Commits
        try:
            for page in client.list_commits(owner, name, cfg.username):
                for c in page:
                    sha = c.get("sha")
                    if not sha or sha in seen_shas:
                        continue
                    if cfg.exclude_merge_commits and is_merge_commit(c):
                        continue

                    date_str = c.get("commit", {}).get("author", {}).get("date")
                    if not date_str:
                        continue

                    seen_shas.add(sha)
                    utc_dt = parse_utc_timestamp(date_str)
                    local_dt = utc_dt.astimezone(tz)

                    total_commits += 1
                    tod[bucket_time_of_day(local_dt)] += 1
                    weekday[WEEKDAYS[local_dt.weekday()]] += 1
        except Exception:
            logging.exception("Failed to fetch commits for %s/%s", owner, name)

    return total_commits, tod, weekday, dict(lang_bytes)


def render_markdown(
    total_commits: int,
    tod: Counter,
    weekday: Counter,
    lang_bytes: Dict[str, int],
) -> str:
    # Time of day
    tod_rows: List[Tuple[str, int, float]] = []
    for name, _, _ in TIME_BUCKETS:
        count = int(tod.get(name, 0))
        tod_rows.append((name, count, to_percent(count, total_commits)))

    # Weekday
    weekday_rows: List[Tuple[str, int, float]] = []
    for day in WEEKDAYS:
        count = int(weekday.get(day, 0))
        weekday_rows.append((day, count, to_percent(count, total_commits)))

    # Languages
    total_bytes = sum(int(v) for v in lang_bytes.values())
    lang_rows: List[Tuple[str, int, float]] = []
    for lang, b in sorted(lang_bytes.items(), key=lambda x: x[1], reverse=True):
        lang_rows.append((lang, int(b), round((b / total_bytes * 100.0), 2) if total_bytes else 0.0))

    if len(lang_rows) > 8:
        top = lang_rows[:8]
        other_pct = round(max(0.0, 100.0 - sum(r[2] for r in top)), 2)
        top_bytes = sum(r[1] for r in top)
        other_bytes = max(0, total_bytes - top_bytes)
        lang_rows = top + [("Other", other_bytes, other_pct)]

    md = "".join(
        [
            "## 📊 GitHub Analytics\n\n",
            f"**Total Commits:** {total_commits}\n\n",
            "### ⏰ Productivity by Time\n",
            format_kv_lines(tod_rows, label_width=10) + "\n\n",
            "### 📅 Productivity by Weekday\n",
            format_kv_lines(weekday_rows, label_width=10) + "\n\n",
            "### 💻 Language Usage\n",
            (format_lang_lines(lang_rows, label_width=12) if lang_rows else "No language data.")
            + "\n",
        ]
    )
    return md.rstrip("\n")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="GitHub Analytics Engine")
    p.add_argument("--username", required=True)
    p.add_argument("--timezone", default="Asia/Kolkata")
    p.add_argument("--include-private", action="store_true")
    p.add_argument("--exclude-forks", action="store_true")
    p.add_argument("--exclude-merge-commits", action="store_true")
    p.add_argument("--readme", default="README.md")
    return p


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = build_arg_parser().parse_args()
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("Missing GITHUB_TOKEN (or GH_TOKEN) environment variable")

    cfg = Config(
        username=args.username,
        timezone=args.timezone,
        include_private=bool(args.include_private),
        exclude_forks=bool(args.exclude_forks),
        exclude_merge_commits=bool(args.exclude_merge_commits),
    )

    client = GitHubClient(token=token)

    # If include_private is requested, validate token scope by calling /user
    if cfg.include_private:
        try:
            _ = client.get_authenticated_user()
        except Exception as e:
            raise RuntimeError(
                "include-private requested but token cannot access /user. Use a PAT with repo scope."
            ) from e

    total_commits, tod, weekday, lang_bytes = aggregate(client, cfg)
    new_md = render_markdown(total_commits, tod, weekday, lang_bytes)

    from pathlib import Path

    updater = ReadmeUpdater(
        readme_path=Path(args.readme),
        section=ReadmeSection(
            start_marker="<!--START_SECTION:github_stats-->",
            end_marker="<!--END_SECTION:github_stats-->",
        ),
    )

    changed = updater.update_section(new_md)
    logging.info("README updated: %s", changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
