import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple

import requests


@dataclass(frozen=True)
class DayPoint:
    day: date
    count: int


def _github_graphql(token: str, query: str, variables: Dict) -> Dict:
    resp = requests.post(
        "https://api.github.com/graphql",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
        json={"query": query, "variables": variables},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(str(data["errors"]))
    return data["data"]


def fetch_daily_contributions(token: str, username: str, days: int) -> List[DayPoint]:
    today = datetime.now(timezone.utc).date()
    from_day = today - timedelta(days=days - 1)

    query = """
    query($login: String!, $from: DateTime!, $to: DateTime!) {
      user(login: $login) {
        contributionsCollection(from: $from, to: $to) {
          contributionCalendar {
            weeks {
              contributionDays {
                date
                contributionCount
              }
            }
          }
        }
      }
    }
    """

    variables = {
        "login": username,
        "from": datetime(from_day.year, from_day.month, from_day.day, tzinfo=timezone.utc).isoformat(),
        "to": datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=timezone.utc).isoformat(),
    }

    data = _github_graphql(token, query, variables)
    user = data.get("user")
    if not user:
        raise RuntimeError(f"User not found: {username}")

    weeks = (
        user.get("contributionsCollection", {})
        .get("contributionCalendar", {})
        .get("weeks", [])
    )

    points: List[DayPoint] = []
    for w in weeks:
        for d in w.get("contributionDays", []):
            d_str = d.get("date")
            c = int(d.get("contributionCount") or 0)
            if not d_str:
                continue
            points.append(DayPoint(day=date.fromisoformat(d_str), count=c))

    points.sort(key=lambda p: p.day)
    wanted = [p for p in points if from_day <= p.day <= today]

    idx = 0
    filled: List[DayPoint] = []
    cur = from_day
    while cur <= today:
        if idx < len(wanted) and wanted[idx].day == cur:
            filled.append(wanted[idx])
            idx += 1
        else:
            filled.append(DayPoint(day=cur, count=0))
        cur += timedelta(days=1)

    return filled


def compute_streaks(points: List[DayPoint]) -> Tuple[int, int, int]:
    if not points:
        return 0, 0, 0

    total = sum(p.count for p in points)

    longest = 0
    run = 0
    for p in points:
        if p.count > 0:
            run += 1
            if run > longest:
                longest = run
        else:
            run = 0

    current = 0
    for p in reversed(points):
        if p.count > 0:
            current += 1
        else:
            break

    return total, current, longest


def _fmt_day_range(points: List[DayPoint]) -> str:
    if not points:
        return ""
    start = points[0].day
    end = points[-1].day
    if start == end:
        return start.strftime("%b %-d") if os.name != "nt" else start.strftime("%b %d").replace(" 0", " ")
    s = start.strftime("%b %-d") if os.name != "nt" else start.strftime("%b %d").replace(" 0", " ")
    e = end.strftime("%b %-d") if os.name != "nt" else end.strftime("%b %d").replace(" 0", " ")
    return f"{s} - {e}"


def render_streak_svg(username: str, total: int, current: int, longest: int, range_label: str) -> str:
    w = 500
    h = 200
    bg = "#0D1117"
    fg = "#FEFEFE"
    accent = "#58A6FF"
    muted = "#9E9E9E"
    border = "#1F6FEB"

    def text(x: int, y: int, s: str, size: int, weight: int, fill: str, anchor: str = "middle") -> str:
        return (
            f"<text x='{x}' y='{y}' text-anchor='{anchor}' fill='{fill}' "
            f"font-family='Segoe UI, Ubuntu, sans-serif' font-weight='{weight}' font-size='{size}px'>{s}</text>"
        )

    svg = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' viewBox='0 0 {w} {h}'>",
        f"<rect x='0' y='0' width='{w}' height='{h}' rx='10' fill='{bg}' stroke='{border}' stroke-width='1'/>",
        f"<line x1='{w/3}' y1='30' x2='{w/3}' y2='{h-25}' stroke='{border}' stroke-width='1' opacity='0.5'/>",
        f"<line x1='{2*w/3}' y1='30' x2='{2*w/3}' y2='{h-25}' stroke='{border}' stroke-width='1' opacity='0.5'/>",
        text(int(w / 6), 70, str(total), 34, 700, accent),
        text(int(w / 6), 100, "Total Contributions", 14, 400, fg),
        text(int(w / 6), 125, range_label, 12, 400, muted),
        text(int(w / 2), 70, str(current), 34, 700, fg),
        text(int(w / 2), 100, "Current Streak", 14, 700, fg),
        text(int(w / 2), 125, "Today", 12, 400, muted),
        text(int(5 * w / 6), 70, str(longest), 34, 700, accent),
        text(int(5 * w / 6), 100, "Longest Streak", 14, 400, fg),
        text(int(5 * w / 6), 125, "", 12, 400, muted),
        text(int(w / 2), 180, username, 12, 600, muted),
        "</svg>",
    ]
    return "\n".join(svg)


def render_activity_graph_svg(points: List[DayPoint], title: str) -> str:
    w = 1200
    h = 420
    pad_l = 90
    pad_r = 50
    pad_t = 80
    pad_b = 70

    bg = "#20232a"
    fg = "#5bcdec"
    line = "#5bcdec"
    point = "#ffffff"

    n = len(points)
    if n <= 1:
        n = 2

    max_c = max((p.count for p in points), default=0)
    max_c = max(max_c, 1)

    plot_w = w - pad_l - pad_r
    plot_h = h - pad_t - pad_b

    def x(i: int) -> float:
        return pad_l + (i * plot_w / (len(points) - 1 if len(points) > 1 else 1))

    def y(v: int) -> float:
        return pad_t + plot_h - (v / max_c) * plot_h

    path_parts: List[str] = []
    for i, p in enumerate(points):
        xi = x(i)
        yi = y(p.count)
        path_parts.append(f"{xi:.2f},{yi:.2f}")

    d = "M" + " L".join(path_parts)

    labels_every = max(1, len(points) // 30)
    label_nodes: List[str] = []
    for i, p in enumerate(points):
        if i % labels_every != 0 and i != len(points) - 1:
            continue
        xi = x(i)
        label_nodes.append(
            f"<text x='{xi:.2f}' y='{pad_t + plot_h + 35}' text-anchor='middle' fill='{fg}' font-family='Segoe UI, Ubuntu, sans-serif' font-size='12px'>{p.day.day}</text>"
        )

    pts = [
        f"<circle cx='{x(i):.2f}' cy='{y(p.count):.2f}' r='4' fill='{point}' opacity='0.95'/>"
        for i, p in enumerate(points)
    ]

    svg = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' height='{h}' viewBox='0 0 {w} {h}'>",
        f"<rect x='0' y='0' width='{w}' height='{h}' rx='0' fill='{bg}'/>",
        f"<text x='{w/2}' y='45' text-anchor='middle' fill='{fg}' font-family='Segoe UI, Ubuntu, sans-serif' font-weight='700' font-size='20px'>{title}</text>",
        f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{pad_t + plot_h}' stroke='{fg}' opacity='0.25'/>",
        f"<line x1='{pad_l}' y1='{pad_t + plot_h}' x2='{pad_l + plot_w}' y2='{pad_t + plot_h}' stroke='{fg}' opacity='0.25'/>",
        f"<path d='{d}' fill='none' stroke='{line}' stroke-width='4' opacity='0.95'/>",
        *pts,
        *label_nodes,
        f"<text x='{w/2}' y='{h-20}' text-anchor='middle' fill='{fg}' font-family='Segoe UI, Ubuntu, sans-serif' font-size='14px'>Days</text>",
        f"<text x='22' y='{pad_t + plot_h/2}' transform='rotate(-90 22 {pad_t + plot_h/2})' text-anchor='middle' fill='{fg}' font-family='Segoe UI, Ubuntu, sans-serif' font-size='14px'>Contributions</text>",
        "</svg>",
    ]
    return "\n".join(svg)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--username", required=True)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--graph-days", type=int, default=30)
    ap.add_argument("--out-dir", default="assets")
    args = ap.parse_args()

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_PAT")
    if not token:
        raise RuntimeError("Missing GH_TOKEN or GITHUB_TOKEN")

    os.makedirs(args.out_dir, exist_ok=True)

    points_year = fetch_daily_contributions(token, args.username, args.days)
    total, current, longest = compute_streaks(points_year)
    range_label = _fmt_day_range(points_year)

    streak_svg = render_streak_svg(args.username, total=total, current=current, longest=longest, range_label=range_label)
    with open(os.path.join(args.out_dir, "streak.svg"), "w", encoding="utf-8") as f:
        f.write(streak_svg)

    points_graph = fetch_daily_contributions(token, args.username, args.graph_days)
    graph_svg = render_activity_graph_svg(points_graph, title=f"{args.username}'s Contribution Graph")
    with open(os.path.join(args.out_dir, "activity-graph.svg"), "w", encoding="utf-8") as f:
        f.write(graph_svg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
