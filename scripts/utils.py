import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple


def safe_div(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def to_percent(part: int, total: int, digits: int = 2) -> float:
    return round(safe_div(part, total) * 100.0, digits)


def format_kv_lines(rows: Iterable[Tuple[str, int, float]], label_width: int = 10) -> str:
    lines: List[str] = []
    for label, count, pct in rows:
        lines.append(f"{label.ljust(label_width)} – {count} commits ({pct:.2f}%)")
    return "\n".join(lines)


def format_lang_lines(rows: Iterable[Tuple[str, int, float]], label_width: int = 12) -> str:
    lines: List[str] = []
    for label, bytes_count, pct in rows:
        lines.append(f"{label.ljust(label_width)} – {pct:.2f}%")
    return "\n".join(lines)


@dataclass(frozen=True)
class ReadmeSection:
    start_marker: str
    end_marker: str

    def replace(self, content: str, new_inner: str) -> str:
        pattern = re.compile(
            rf"({re.escape(self.start_marker)}\n)(.*?)(\n{re.escape(self.end_marker)})",
            re.DOTALL,
        )

        match = pattern.search(content)
        if not match:
            raise ValueError(
                "README markers not found. Please add start/end markers before running automation."
            )

        return pattern.sub(rf"\1{new_inner}\3", content, count=1)
