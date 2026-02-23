"""DBLens CLI entry point."""
from __future__ import annotations
import sys
from typing import Optional

import typer
from rich.console import Console

app = typer.Typer(
    name="dblens",
    help="🔍 DBLens — database optimization insights CLI",
    no_args_is_help=True,
    add_completion=False,
)
console = Console(stderr=True)

# ── Shared options ────────────────────────────────────────────────────────────
_json_opt   = typer.Option(False, "--json",        help="Output compact JSON")
_pretty_opt = typer.Option(False, "--json-pretty", help="Output pretty JSON")
_limit_opt  = typer.Option(20,   "--limit",        help="Max slow queries to fetch")
_long_opt   = typer.Option(5,    "--long-threshold", help="Long-running threshold (seconds)")


def _run(connector, db_type: str, target: str, json_out: bool, pretty: bool, limit: int, long_thresh: int):
    from dblens.analyzers.core import Analyzer
    from dblens import renderer

    connector.slow_queries.__func__ if hasattr(connector.slow_queries, "__func__") else None
    # patch limit into slow_queries call
    original_slow = connector.slow_queries
    connector.slow_queries = lambda: original_slow(limit)
    original_long = connector.long_running
    connector.long_running = lambda: original_long(long_thresh)

    with console.status("[bold cyan]Analyzing database…[/bold cyan]"):
        analyzer = Analyzer(connector)
        findings = analyzer.run()

    connector.close()

    if json_out or pretty:
        renderer.render_json(findings, db_type, target)
    else:
        renderer.render_header(db_type, target)
        renderer.render_summary(findings)
        renderer.render_findings(findings)


# ── PostgreSQL ────────────────────────────────────────────────────────────────
@app.command("postgres", help="Analyze a PostgreSQL database.")
def cmd_postgres(
    dsn: str = typer.Argument(..., help="Connection string. e.g. postgresql://user:pass@host:5432/db"),
    json:        bool = _json_opt,
    json_pretty: bool = _pretty_opt,
    limit:       int  = _limit_opt,
    long_threshold: int = _long_opt,
):
    from dblens.connectors.postgres import PostgresConnector
    try:
        conn = PostgresConnector(dsn)
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        raise typer.Exit(1)
    _run(conn, "postgresql", dsn.split("@")[-1], json, json_pretty, limit, long_threshold)


# ── MySQL / MariaDB ───────────────────────────────────────────────────────────
@app.command("mysql", help="Analyze a MySQL or MariaDB database.")
def cmd_mysql(
    host:     str = typer.Option("localhost", "--host",     help="Hostname"),
    port:     int = typer.Option(3306,        "--port",     help="Port"),
    user:     str = typer.Option(...,         "--user",     "-u", help="Username"),
    password: str = typer.Option(...,         "--password", "-p", prompt=True, hide_input=True),
    database: str = typer.Option(...,         "--database", "-d", help="Database name"),
    json:        bool = _json_opt,
    json_pretty: bool = _pretty_opt,
    limit:       int  = _limit_opt,
    long_threshold: int = _long_opt,
):
    from dblens.connectors.mysql import MySQLConnector
    try:
        conn = MySQLConnector(host, port, user, password, database)
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        raise typer.Exit(1)
    _run(conn, "mysql", f"{host}/{database}", json, json_pretty, limit, long_threshold)


# ── SQLite ────────────────────────────────────────────────────────────────────
@app.command("sqlite", help="Analyze a SQLite database file.")
def cmd_sqlite(
    path: str = typer.Argument(..., help="Path to .db / .sqlite file"),
    json:        bool = _json_opt,
    json_pretty: bool = _pretty_opt,
    limit:       int  = _limit_opt,
    long_threshold: int = _long_opt,
):
    from dblens.connectors.sqlite import SQLiteConnector
    try:
        conn = SQLiteConnector(path)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    _run(conn, "sqlite", path, json, json_pretty, limit, long_threshold)


# ── MongoDB ───────────────────────────────────────────────────────────────────
@app.command("mongo", help="Analyze a MongoDB database.")
def cmd_mongo(
    uri:      str = typer.Argument(..., help="MongoDB URI. e.g. mongodb://user:pass@host:27017"),
    database: str = typer.Option(..., "--database", "-d", help="Database name"),
    json:        bool = _json_opt,
    json_pretty: bool = _pretty_opt,
    limit:       int  = _limit_opt,
    long_threshold: int = _long_opt,
):
    from dblens.connectors.mongo import MongoConnector
    try:
        conn = MongoConnector(uri, database)
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        raise typer.Exit(1)
    _run(conn, "mongodb", f"{uri.split('@')[-1]}/{database}", json, json_pretty, limit, long_threshold)


# ── Snowflake ─────────────────────────────────────────────────────────────────
@app.command("snowflake", help="Analyze a Snowflake data warehouse.")
def cmd_snowflake(
    account:   str = typer.Option(..., "--account",   help="Snowflake account identifier"),
    user:      str = typer.Option(..., "--user",      "-u"),
    password:  str = typer.Option(..., "--password",  "-p", prompt=True, hide_input=True),
    database:  str = typer.Option(..., "--database",  "-d"),
    warehouse: str = typer.Option(..., "--warehouse", "-w"),
    schema:    str = typer.Option("PUBLIC", "--schema"),
    json:        bool = _json_opt,
    json_pretty: bool = _pretty_opt,
    limit:       int  = _limit_opt,
    long_threshold: int = _long_opt,
):
    from dblens.connectors.snowflake import SnowflakeConnector
    try:
        conn = SnowflakeConnector(account, user, password, database, warehouse, schema)
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        raise typer.Exit(1)
    _run(conn, "snowflake", f"{account}/{database}", json, json_pretty, limit, long_threshold)


def main():
    app()


if __name__ == "__main__":
    main()
