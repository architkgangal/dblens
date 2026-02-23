"""Core analysis engine - turns raw DB data into scored recommendations."""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING  = "WARNING"
    INFO     = "INFO"
    OK       = "OK"


@dataclass
class Finding:
    category: str
    severity: Severity
    title: str
    detail: str
    recommendation: str
    metric: dict[str, Any] = field(default_factory=dict)


def _sev(val: float, warn: float, crit: float, higher_is_worse: bool = True) -> Severity:
    if higher_is_worse:
        if val >= crit: return Severity.CRITICAL
        if val >= warn: return Severity.WARNING
        return Severity.OK
    else:
        if val <= crit: return Severity.CRITICAL
        if val <= warn: return Severity.WARNING
        return Severity.OK


class Analyzer:
    def __init__(self, connector):
        self.conn = connector
        self.findings: list[Finding] = []

    def run(self) -> list[Finding]:
        self.findings = []
        self._check_slow_queries()
        self._check_missing_indexes()
        self._check_bloat()
        self._check_resources()
        self._check_index_usage()
        self._check_long_running()
        self.findings.sort(
            key=lambda f: ["CRITICAL", "WARNING", "INFO", "OK"].index(f.severity.value)
        )
        return self.findings

    # ── Slow Queries ──────────────────────────────────────────────────────────
    def _check_slow_queries(self):
        rows = self.conn.slow_queries()
        for row in rows[:10]:
            mean = float(
                row.get("mean_ms") or row.get("mean_sec", 0) or
                row.get("millis", 0) or row.get("exec_sec", 0) or 0
            )
            if row.get("mean_sec") or row.get("exec_sec"):
                mean *= 1000
            if mean < 1:
                continue
            sev = _sev(mean, warn=100, crit=500)
            q = str(
                row.get("query") or row.get("query_text") or
                row.get("DIGEST_TEXT") or ""
            )[:80]
            calls = row.get("calls") or row.get("COUNT_STAR") or "n/a"
            self.findings.append(Finding(
                category="slow_query",
                severity=sev,
                title=f"Slow query ({mean:.1f} ms avg)",
                detail=f"Query: {q!r}  |  rows scanned: {row.get('rows', 'n/a')}",
                recommendation=(
                    "Review EXPLAIN QUERY PLAN. Add an index on the filter/join columns. "
                    "Consider caching frequent read-only results."
                ),
                metric={"mean_ms": mean, "calls": calls},
            ))

    # ── Missing Indexes ───────────────────────────────────────────────────────
    def _check_missing_indexes(self):
        rows = self.conn.missing_indexes()
        for row in rows[:10]:
            table = (
                row.get("table_name") or row.get("table") or
                row.get("object_name") or row.get("collection", "")
            )
            scans = row.get("seq_scan") or row.get("full_scans") or row.get("partitions_scanned") or 0
            self.findings.append(Finding(
                category="missing_index",
                severity=Severity.WARNING,
                title=f"Missing index on `{table}`",
                detail=f"{scans:,} rows scanned without an index",
                recommendation=(
                    f"Run EXPLAIN QUERY PLAN on frequent queries against `{table}`. "
                    "Add a composite index on columns used in WHERE / JOIN / ORDER BY."
                ),
                metric={"table": table, "scans": scans},
            ))

    # ── Bloat / Storage ───────────────────────────────────────────────────────
    def _check_bloat(self):
        rows = self.conn.table_bloat()
        if not rows:
            return
        for row in rows:
            table    = row.get("table_name") or row.get("collection", "unknown")
            dead_pct = float(row.get("dead_pct", 0) or 0)

            # Dead-page / freelist bloat (Postgres dead tuples OR SQLite freelist)
            if dead_pct > 0:
                sev = _sev(dead_pct, warn=5, crit=20)
                wasted = row.get("wasted_mb", "")
                wasted_str = f"  |  wasted: {wasted} MB" if wasted else ""
                label = "freelist pages" if table == "__database__" else "dead rows"
                self.findings.append(Finding(
                    category="bloat",
                    severity=sev,
                    title=f"Storage bloat: {dead_pct:.1f}% wasted ({table})",
                    detail=f"{row.get('n_dead_tup', 0):,} {label}{wasted_str}",
                    recommendation=(
                        "Run VACUUM to reclaim wasted space. "
                        "For SQLite, run: sqlite3 mydb.db 'VACUUM;'. "
                        "For Postgres: VACUUM ANALYZE <table>. "
                        "Check that autovacuum is enabled."
                    ),
                    metric={"table": table, "dead_pct": dead_pct},
                ))

            # Large table size warning
            size_mb = float(
                row.get("size_mb") or row.get("storage_mb") or row.get("total_mb") or 0
            )
            if size_mb > 5 and table != "__database__":
                self.findings.append(Finding(
                    category="bloat",
                    severity=Severity.INFO,
                    title=f"Large table `{table}` ({size_mb:.1f} MB)",
                    detail=f"Rows: {row.get('n_live_tup', row.get('documents', 'n/a')):,}",
                    recommendation=(
                        "Consider archiving old data, adding TTL policies, or partitioning "
                        "the table by date/range to keep it manageable."
                    ),
                    metric={"table": table, "size_mb": size_mb},
                ))

    # ── Resource Usage ────────────────────────────────────────────────────────
    def _check_resources(self):
        data = self.conn.resource_usage()

        # Cache hit rate (Postgres, MySQL, SQLite coverage estimate)
        ch = data.get("cache_hit")
        if ch and ch[0].get("cache_hit_pct") is not None:
            pct = float(ch[0]["cache_hit_pct"] or 0)
            sev = _sev(pct, warn=80, crit=50, higher_is_worse=False)
            label = (
                "Cache covers only" if data.get("sqlite") else "Buffer cache hit rate"
            )
            suffix = (
                " of DB file (cache too small)" if data.get("sqlite") else "%"
            )
            if sev != Severity.OK:
                self.findings.append(Finding(
                    category="resource",
                    severity=sev,
                    title=f"Low cache coverage: {pct:.1f}%",
                    detail=f"{label} {pct:.1f}%{suffix}",
                    recommendation=(
                        "Increase PRAGMA cache_size in SQLite (e.g. PRAGMA cache_size=-64000 for 64MB). "
                        "For Postgres: increase shared_buffers. For MySQL: innodb_buffer_pool_size."
                    ),
                    metric={"cache_hit_pct": pct},
                ))

        # SQLite-specific: journal mode warning
        sq = data.get("sqlite", {})
        if sq.get("journal_mode") == "delete":
            self.findings.append(Finding(
                category="resource",
                severity=Severity.WARNING,
                title="SQLite using slow DELETE journal mode",
                detail="journal_mode=delete causes full-file sync on every write",
                recommendation=(
                    "Switch to WAL mode for much better write performance: "
                    "PRAGMA journal_mode=WAL;"
                ),
                metric={"journal_mode": "delete"},
            ))

        # SQLite: freelist warning via resource
        if sq.get("freelist_pages", 0) > 100:
            fp = sq["freelist_pages"]
            self.findings.append(Finding(
                category="resource",
                severity=Severity.INFO,
                title=f"SQLite has {fp:,} unused freelist pages",
                detail="Pages freed by DELETE but not yet returned to OS",
                recommendation="Run: sqlite3 yourdb.db 'VACUUM;' to compact the file.",
                metric={"freelist_pages": fp},
            ))

        # Blocked locks (Postgres/MySQL)
        locks = data.get("locks")
        if locks and locks[0].get("blocked_queries", 0):
            n = int(locks[0]["blocked_queries"])
            if n > 0:
                self.findings.append(Finding(
                    category="resource",
                    severity=Severity.CRITICAL if n > 5 else Severity.WARNING,
                    title=f"{n} queries blocked by locks",
                    detail="Queries are waiting for row/table locks",
                    recommendation=(
                        "Run pg_blocking_pids() to find the blocker. "
                        "Use shorter transactions and row-level locking."
                    ),
                    metric={"blocked": n},
                ))

        # MongoDB memory
        mem = data.get("memory_mb", {})
        if mem.get("resident", 0) > 0:
            self.findings.append(Finding(
                category="resource",
                severity=Severity.INFO,
                title=f"MongoDB memory: {mem['resident']} MB resident / {mem.get('virtual', 0)} MB virtual",
                detail="",
                recommendation="Ensure WiredTiger cache is ~50% of available RAM.",
                metric=mem,
            ))

        # Snowflake credits
        for wh in data.get("warehouse_credits_7d", [])[:3]:
            c = float(wh.get("total_credits") or wh.get("TOTAL_CREDITS") or 0)
            name = wh.get("warehouse_name") or wh.get("WAREHOUSE_NAME", "")
            if c > 100:
                self.findings.append(Finding(
                    category="resource",
                    severity=Severity.WARNING,
                    title=f"High Snowflake credit usage: {name} ({c:.0f} credits / 7d)",
                    detail="",
                    recommendation=(
                        "Review auto-suspend settings, downsize the warehouse, "
                        "and investigate heavy repeated queries."
                    ),
                    metric={"warehouse": name, "credits_7d": c},
                ))

    # ── Index Usage ───────────────────────────────────────────────────────────
    def _check_index_usage(self):
        if not hasattr(self.conn, "index_usage"):
            return
        rows = self.conn.index_usage()
        for row in rows:
            idx_scan = row.get("idx_scan")         # Postgres
            scan_ms  = row.get("scan_ms")          # SQLite
            iname    = row.get("index_name") or row.get("indexrelname", "")
            tname    = row.get("table_name") or row.get("relname", "")
            cols     = row.get("columns", "")
            size     = row.get("index_size", "")

            # Postgres: flag never-used indexes
            if idx_scan is not None and int(idx_scan) == 0:
                self.findings.append(Finding(
                    category="index_usage",
                    severity=Severity.WARNING,
                    title=f"Unused index `{iname}` on `{tname}`",
                    detail=f"0 scans since last stats reset  |  size: {size}",
                    recommendation=(
                        "If this index has never been used, consider dropping it. "
                        "Unused indexes waste space and slow down writes. "
                        "Verify with pg_stat_reset() then monitor for a week."
                    ),
                    metric={"index": iname, "table": tname, "scans": 0},
                ))

            # SQLite: report index scan timing as INFO
            if scan_ms is not None:
                rows_count = row.get("table_rows", 0)
                self.findings.append(Finding(
                    category="index_usage",
                    severity=Severity.INFO,
                    title=f"Index `{iname}` on `{tname}` ({cols})",
                    detail=f"Full index scan: {scan_ms} ms  |  table rows: {rows_count:,}",
                    recommendation=(
                        "Verify this index is actually used by your queries with "
                        "EXPLAIN QUERY PLAN. Drop it if never referenced to speed up writes."
                    ),
                    metric={"index": iname, "table": tname, "scan_ms": scan_ms},
                ))

    # ── Long Running ──────────────────────────────────────────────────────────
    def _check_long_running(self):
        rows = self.conn.long_running()
        for row in rows:
            dur = str(
                row.get("duration") or row.get("TIME") or
                row.get("secs_running") or row.get("exec_sec", "")
            )
            q = str(row.get("query") or row.get("query_text") or "")[:80]
            self.findings.append(Finding(
                category="long_running",
                severity=Severity.WARNING,
                title=f"Slow/expensive query pattern detected ({dur})",
                detail=f"Query: {q!r}",
                recommendation=(
                    "Add indexes on filter columns to avoid full scans. "
                    "Rewrite JOINs to use indexed columns. "
                    "Use LIMIT clauses to prevent runaway result sets."
                ),
                metric={"duration": dur},
            ))