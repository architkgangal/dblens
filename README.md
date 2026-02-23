# DBLens — Database Optimization Insights CLI

A lightweight Python CLI that connects to your databases and surfaces **actionable optimization insights** — slow queries, missing indexes, table bloat, resource pressure, and long-running operations — all in one command.

Supports **PostgreSQL · MySQL/MariaDB · SQLite · MongoDB · Snowflake**.

---

## What it analyzes

| Category | What DBLens looks for |
|---|---|
|  **Slow Queries** | Top queries by average execution time, with call counts |
|  **Missing Indexes** | Tables with high sequential scan rates vs index usage |
|  **Storage / Bloat** | Dead rows (Postgres), fragmentation (MySQL), large collections (Mongo) |
|  **Resource Usage** | Buffer cache hit rate, connection counts, lock contention, memory, credits |
|  **Long-Running Queries** | Currently active queries exceeding a configurable threshold |

Every finding comes with:
- **Severity** — CRITICAL / WARNING / INFO
- **Plain-English explanation**
- **Concrete recommendation** on how to fix it

---

##  Installation

```bash
pip install dblens

# Install with drivers for your database(s):
pip install "dblens[postgres]"
pip install "dblens[mysql]"
pip install "dblens[mongo]"
pip install "dblens[snowflake]"
pip install "dblens[all]"       # everything
```

Or from source:
```bash
git clone https://github.com/yourname/dblens
cd dblens
pip install -e ".[all]"
```

---

##  Quick Start

### PostgreSQL
```bash
dblens postgres "postgresql://user:pass@localhost:5432/mydb"
```

### MySQL / MariaDB
```bash
dblens mysql --host localhost --user root --database myapp
# (password will be prompted securely)
```

### SQLite
```bash
dblens sqlite /path/to/my_database.db
```

### MongoDB
```bash
dblens mongo "mongodb://user:pass@localhost:27017" --database myapp
```

### Snowflake
```bash
dblens snowflake \
  --account myorg-myaccount \
  --user analyst \
  --database PROD \
  --warehouse COMPUTE_WH
```

---

##  Usage & Flags

All subcommands share these flags:

```
--limit INT               Max slow queries to fetch (default: 20)
--long-threshold INT      Seconds to consider a query "long-running" (default: 5)
--json                    Output compact JSON
--json-pretty             Output pretty-printed JSON
```

### Examples

```bash
# Human-readable (default)
dblens postgres "postgresql://..." 

# JSON for piping into other tools
dblens postgres "postgresql://..." --json

# More aggressive slow query hunting
dblens postgres "postgresql://..." --limit 50 --long-threshold 2

# Snowflake with all options
dblens snowflake --account myorg --user alice --database DWH \
  --warehouse LARGE_WH --schema ANALYTICS --json-pretty
```

---

##  Sample Output

```
╔══════════════════════════════════════════════════════╗
║   DBLens  ·  POSTGRESQL  ·  localhost/myapp        ║
║  Analyzed at 2026-02-23 14:32:01                     ║
╚══════════════════════════════════════════════════════╝

╭──────────────╮
│   Summary    │
│ 🔴 CRITICAL  │ 2
│ 🟡 WARNING   │ 5
│ 🔵 INFO      │ 1
╰──────────────╯

──────────────────── Slow Queries ───────────────────────
┌──────────┬──────────────────────────────┬──────────────┬───────────────────────────────┐
│ Severity │ Finding                      │ Detail       │ Recommendation                │
├──────────┼──────────────────────────────┼──────────────┼───────────────────────────────┤
│ 🔴 CRIT  │ Slow query (4,821 ms avg)    │ 'SELECT ...  │ Review EXPLAIN plan. Consider │
│ 🟡 WARN  │ Slow query (892 ms avg)      │ 'UPDATE ...  │ Add index on filter columns.  │
└──────────┴──────────────────────────────┴──────────────┴───────────────────────────────┘

──────────────────── Missing Indexes ────────────────────
...
```

---

##  JSON Output Schema

```json
{
  "dblens_version": "0.1.0",
  "db_type": "postgresql",
  "target": "localhost/myapp",
  "analyzed_at": "2026-02-23T14:32:01Z",
  "findings": [
    {
      "category": "slow_query",
      "severity": "CRITICAL",
      "title": "Slow query (4821 ms avg)",
      "detail": "Query: 'SELECT ...'  |  calls: 1203",
      "recommendation": "Review EXPLAIN plan...",
      "metric": { "mean_ms": 4821, "calls": 1203 }
    }
  ]
}
```

---

##  Architecture

```
dblens/
├── cli.py              # Typer CLI — one subcommand per database
├── analyzers/
│   └── core.py         # Generic Analyzer — runs all checks, produces Findings
├── connectors/
│   ├── postgres.py     # PostgreSQL queries (pg_stat_statements, pg_stat_user_tables…)
│   ├── mysql.py        # MySQL performance_schema queries
│   ├── sqlite.py       # SQLite PRAGMA + EXPLAIN QUERY PLAN
│   ├── mongo.py        # MongoDB serverStatus, collStats, system.profile
│   └── snowflake.py    # Snowflake account_usage views
└── renderer.py         # Rich terminal tables + JSON output
```

### Adding a new database

1. Create `dblens/connectors/mydb.py` implementing these methods:
   - `slow_queries(limit)` → `list[dict]`
   - `missing_indexes()` → `list[dict]`
   - `table_bloat()` → `list[dict]`
   - `resource_usage()` → `dict`
   - `long_running(threshold_sec)` → `list[dict]`
   - `close()`

2. Add a `@app.command("mydb")` in `cli.py`.

That's it — the `Analyzer` and renderer work with any connector automatically.

---

##  Required Permissions

| Database | Required permissions |
|---|---|
| PostgreSQL | `pg_monitor` role (or `pg_read_all_stats`) + `pg_stat_statements` extension enabled |
| MySQL | `SELECT` on `performance_schema` |
| SQLite | Read access to the `.db` file |
| MongoDB | `clusterMonitor` role on `admin` + profiling enabled (`db.setProfilingLevel(1)`) |
| Snowflake | `MONITOR` privilege on warehouse + access to `snowflake.account_usage` schema |

---

## License

MIT
