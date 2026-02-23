"""Terminal output renderer using Rich."""
from __future__ import annotations
from datetime import datetime
from typing import Sequence

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from rich.rule import Rule

from dblens.analyzers.core import Finding, Severity

console = Console()

SEV_STYLE = {
    Severity.CRITICAL: "bold red",
    Severity.WARNING:  "bold yellow",
    Severity.INFO:     "bold cyan",
    Severity.OK:       "bold green",
}

SEV_ICON = {
    Severity.CRITICAL: "🔴",
    Severity.WARNING:  "🟡",
    Severity.INFO:     "🔵",
    Severity.OK:       "🟢",
}

CAT_LABEL = {
    "slow_query":    "Slow Queries",
    "missing_index": "Missing Indexes",
    "bloat":         "Storage / Bloat",
    "resource":      "Resource Usage",
    "long_running":  "Long-Running Queries",
}


def render_header(db_type: str, target: str):
    console.print()
    console.print(Panel(
        f"[bold white]🔍 DBLens[/bold white]  ·  [cyan]{db_type.upper()}[/cyan]  ·  [dim]{target}[/dim]\n"
        f"[dim]Analyzed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
        box=box.DOUBLE_EDGE,
        style="bold",
        expand=False,
    ))
    console.print()


def render_summary(findings: list[Finding]):
    counts = {s: 0 for s in Severity}
    for f in findings:
        counts[f.severity] += 1

    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    t.add_column(style="dim")
    t.add_column()
    for sev in (Severity.CRITICAL, Severity.WARNING, Severity.INFO, Severity.OK):
        if counts[sev] > 0:
            t.add_row(
                f"{SEV_ICON[sev]} {sev.value}",
                Text(str(counts[sev]), style=SEV_STYLE[sev]),
            )
    console.print(Panel(t, title="[bold]Summary[/bold]", expand=False))
    console.print()


def render_findings(findings: list[Finding]):
    if not findings:
        console.print("[bold green]✅  No issues found![/bold green]")
        return

    # Group by category
    categories: dict[str, list[Finding]] = {}
    for f in findings:
        categories.setdefault(f.category, []).append(f)

    for cat, items in categories.items():
        console.print(Rule(f"[bold]{CAT_LABEL.get(cat, cat)}[/bold]"))
        t = Table(
            box=box.ROUNDED,
            show_lines=True,
            expand=True,
            header_style="bold dim",
        )
        t.add_column("Severity",       width=10)
        t.add_column("Finding",        ratio=2)
        t.add_column("Detail",         ratio=3)
        t.add_column("Recommendation", ratio=4)

        for f in items:
            t.add_row(
                Text(f"{SEV_ICON[f.severity]} {f.severity.value}", style=SEV_STYLE[f.severity]),
                f.title,
                Text(f.detail, style="dim"),
                Text(f.recommendation, style="italic"),
            )
        console.print(t)
        console.print()


def render_json(findings: list[Finding], db_type: str, target: str):
    import json
    out = {
        "dblens_version": "0.1.0",
        "db_type": db_type,
        "target": target,
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
        "findings": [
            {
                "category":       f.category,
                "severity":       f.severity.value,
                "title":          f.title,
                "detail":         f.detail,
                "recommendation": f.recommendation,
                "metric":         f.metric,
            }
            for f in findings
        ],
    }
    console.print(json.dumps(out, indent=2, default=str))
