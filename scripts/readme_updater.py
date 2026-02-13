from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from utils import ReadmeSection


@dataclass(frozen=True)
class ReadmeUpdater:
    readme_path: Path
    section: ReadmeSection

    def update_section(self, new_inner_markdown: str) -> bool:
        content = self.readme_path.read_text(encoding="utf-8")
        updated = self.section.replace(content, new_inner_markdown.rstrip("\n"))
        if updated == content:
            return False
        self.readme_path.write_text(updated, encoding="utf-8")
        return True
