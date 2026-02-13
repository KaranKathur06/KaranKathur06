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
from utils import ReadmeSection, to_percent


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
    def compact_commits_line(labels: List[str], counts: Dict[str, int]) -> str:
        parts: List[str] = []
        for label in labels:
            c = int(counts.get(label, 0))
            parts.append(f"{label} – {c} commits ({to_percent(c, total_commits):.2f}%)")
        return " ".join(parts)

    def compact_lang_line(rows: List[Tuple[str, int]]) -> str:
        total_bytes = sum(b for _, b in rows)
        parts: List[str] = []
        for lang, b in rows:
            pct = round((b / total_bytes * 100.0), 2) if total_bytes else 0.0
            parts.append(f"{lang} – {pct:.2f}%")
        return " ".join(parts) if parts else "No language data."

    time_labels = [name for name, _, _ in TIME_BUCKETS]
    weekday_labels = WEEKDAYS[:]

    lang_sorted: List[Tuple[str, int]] = sorted(
        ((lang, int(b)) for lang, b in lang_bytes.items()), key=lambda x: x[1], reverse=True
    )

    if len(lang_sorted) > 8:
        top = lang_sorted[:8]
        other_bytes = sum(b for _, b in lang_sorted[8:])
        lang_sorted = top + [("Other", other_bytes)]

    md = (
        "## 📊 GitHub Analytics\n\n"
        + f"**Total Commits:** {total_commits}\n\n"
        + "### ⏰ Productivity by Time\n\n"
        + compact_commits_line(time_labels, tod)
        + "\n\n"
        + "### 📅 Productivity by Weekday\n\n"
        + compact_commits_line(weekday_labels, weekday)
        + "\n\n"
        + "### 💻 Language Usage\n\n"
        + compact_lang_line(lang_sorted)
        + "\n"
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
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_PAT")
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

    # If include_private is requested, validate token scope by calling /user.
    # In GitHub Actions, the default GITHUB_TOKEN can be forbidden for /user.
    # In that case, fall back to public-only repo listing instead of failing the workflow.
    if cfg.include_private:
        try:
            _ = client.get_authenticated_user()
        except Exception:
            logging.warning(
                "include-private requested but token cannot access /user. "
                "Falling back to public repositories only. "
                "To include private repos, add a PAT as GH_TOKEN secret with repo scope."
            )
            cfg = Config(
                username=cfg.username,
                timezone=cfg.timezone,
                include_private=False,
                exclude_forks=cfg.exclude_forks,
                exclude_merge_commits=cfg.exclude_merge_commits,
            )

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
