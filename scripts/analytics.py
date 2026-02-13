import argparse
import logging
import os
import sys
import re
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


def aggregate(
    client: GitHubClient,
    cfg: Config,
) -> Tuple[int, Counter, Counter, Dict[str, int]]:
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


def extract_language_allowlist_from_readme(readme_path: str) -> Optional[Set[str]]:
    try:
        content = open(readme_path, "r", encoding="utf-8").read()
    except Exception:
        return None

    # Look for the "**💻 Languages**" block and parse the next non-empty line.
    # Expected format: Python • C • C++ • Java • TypeScript • Dart • SQL
    m = re.search(r"\*\*💻 Languages\*\*\s*\n\s*\n([^\n]+)", content)
    if not m:
        return None

    line = m.group(1).strip()
    parts = [p.strip() for p in line.split("•")]
    parts = [p for p in parts if p]
    if not parts:
        return None
    return set(parts)


def render_markdown(
    total_commits: int,
    tod: Counter,
    weekday: Counter,
    lang_bytes: Dict[str, int],
    language_allowlist: Optional[Set[str]],
) -> str:
    def render_bar_table(
        labels: List[str],
        counts: Dict[str, int],
        total: int,
        width: int,
        value_unit: str,
        label_emojis: Optional[Dict[str, str]] = None,
        pct_overrides: Optional[Dict[str, float]] = None,
    ) -> str:
        max_count = max((int(counts.get(l, 0)) for l in labels), default=0)
        lines: List[str] = []
        for label in labels:
            c = int(counts.get(label, 0))
            pct = float(pct_overrides[label]) if pct_overrides and label in pct_overrides else to_percent(c, total)
            filled = 0 if max_count == 0 else round((c / max_count) * width)
            filled = max(0, min(width, int(filled)))
            bar = "█" * filled + "░" * (width - filled)
            emoji = (label_emojis or {}).get(label, "")
            left = f"{emoji} {label}".strip()
            lines.append(
                f"{left.ljust(12)}  {str(c).rjust(4)} {value_unit}  {bar}  {pct:5.2f} %"
            )
        return "\n".join(lines)

    time_labels = [name for name, _, _ in TIME_BUCKETS]
    weekday_labels = WEEKDAYS[:]

    time_emojis = {
        "Morning": "🌞",
        "Day": "🌆",
        "Evening": "🌃",
        "Night": "🌙",
    }

    dominant_time = max(time_labels, key=lambda t: (int(tod.get(t, 0)), -time_labels.index(t)))
    day_owl_label = {
        "Morning": "Morning",
        "Day": "Day",
        "Evening": "Evening",
        "Night": "Night",
    }.get(dominant_time, "Night")

    most_productive_day = max(
        weekday_labels,
        key=lambda d: (int(weekday.get(d, 0)), -weekday_labels.index(d)),
    )

    # Filter languages to only the ones you claim to work with (from README About Me),
    # and group everything else under Other.
    def normalize_lang(name: str) -> str:
        return name.strip()

    allow = set(normalize_lang(x) for x in language_allowlist) if language_allowlist else None
    allow_map = {
        "C++": "C++",
        "C": "C",
        "Python": "Python",
        "Java": "Java",
        "TypeScript": "TypeScript",
        "JavaScript": "JavaScript",
        "Dart": "Dart",
        "SQL": "SQL",
    }

    filtered_bytes: Dict[str, int] = defaultdict(int)
    other_bytes = 0
    for lang, b in lang_bytes.items():
        lang_norm = normalize_lang(lang)
        display = allow_map.get(lang_norm, lang_norm)
        if allow is not None and display not in allow:
            other_bytes += int(b)
        else:
            filtered_bytes[display] += int(b)

    if other_bytes > 0:
        filtered_bytes["Other"] += other_bytes

    lang_sorted: List[Tuple[str, int]] = sorted(
        ((lang, int(b)) for lang, b in filtered_bytes.items()),
        key=lambda x: x[1],
        reverse=True,
    )

    if len(lang_sorted) > 8:
        top = lang_sorted[:8]
        other_bytes = sum(b for _, b in lang_sorted[8:])
        lang_sorted = top + [("Other", other_bytes)]

    lang_total_bytes = sum(b for _, b in lang_sorted)
    lang_pct_overrides: Dict[str, float] = {}
    for lang, b in lang_sorted:
        lang_pct_overrides[lang] = round((b / lang_total_bytes * 100.0), 2) if lang_total_bytes else 0.0

    # Convert languages list into a counts dict for bar rendering
    lang_counts: Dict[str, int] = {lang: int(b) for lang, b in lang_sorted}

    md = (
        "## 📊 GitHub Analytics\n\n"
        + f"**Total Commits:** {total_commits}\n\n"
        + f"**I'm a {day_owl_label} 🦉**\n\n"
        + "```text\n"
        + render_bar_table(
            time_labels,
            tod,
            total_commits,
            width=25,
            value_unit="commits",
            label_emojis=time_emojis,
        )
        + "\n```\n\n"
        + f"📅 **I'm Most Productive on {most_productive_day}**\n\n"
        + "```text\n"
        + render_bar_table(
            weekday_labels,
            weekday,
            total_commits,
            width=25,
            value_unit="commits",
        )
        + "\n```\n\n"
        + "💻 **Language Usage**\n\n"
        + "```text\n"
        + render_bar_table(
            [lang for lang, _ in lang_sorted],
            lang_counts,
            lang_total_bytes,
            width=25,
            value_unit="bytes",
            pct_overrides=lang_pct_overrides,
        )
        + "\n```\n"
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
    language_allowlist = extract_language_allowlist_from_readme(args.readme)
    new_md = render_markdown(total_commits, tod, weekday, lang_bytes, language_allowlist)

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
