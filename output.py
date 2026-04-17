"""Rendering helpers — keep all formatting concerns in one place."""

from __future__ import annotations

import json
from typing import Iterable

from rich.console import Console
from rich.rule import Rule
from rich.table import Table

_console = Console()

_GREEN = {"SUCCESS", "RUNNING", "READY"}
_RED = {"FAILED", "TIMED_OUT", "ERROR", "NOT_READY"}
_DIM = {"TERMINATED", "PENDING", "RESTARTING"}


def _style(value: str) -> str:
    v = (value or "").upper()
    if v in _GREEN:
        return f"[green]{value}[/green]"
    if v in _RED:
        return f"[red]{value}[/red]"
    if v in _DIM:
        return f"[dim]{value}[/dim]"
    return value


def print_header(title: str) -> None:
    _console.print(Rule(f"[bold cyan]{title}[/bold cyan]"))


def render_table(title: str, rows: list[dict], columns: Iterable[tuple[str, str]]) -> None:
    print_header(title)
    if not rows:
        _console.print(f"[dim]No {title.lower()} found.[/dim]\n")
        return

    table = Table(show_header=True, header_style="bold")
    cols = list(columns)
    for _, display in cols:
        table.add_column(display)

    status_keys = {"state", "result_state"}
    for row in rows:
        table.add_row(*[_style(str(row.get(key, "-"))) if key in status_keys else str(row.get(key, "-")) for key, _ in cols])
    _console.print(table)
    _console.print()


def render_json(sections: dict[str, list[dict]]) -> None:
    print(json.dumps(sections, indent=2, default=str))
