"""SQLite connector for DBLens."""
from __future__ import annotations
import sqlite3
import os
import time


class SQLiteConnector:
    """Connects to a SQLite database file and fetches diagnostic data."""

    def __init__(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQLite file not found: {path}")
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row

    def _q(self, sql: str, params=()) -> list[dict]:
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    # ── Slow Queries ──────────────────────────────────────────────────────────
    # SQLite has no query log, so we benchmark the heaviest full-scan queries
    # by actually timing them against each large unindexed table.
    def slow_queries(self, limit: int = 20) -> list[dict]:
        results = []
        tables = self._q("SELECT name FROM sqlite_master WHERE type='table'")
        indexed = set(
            r["tbl_name"]
            for r in self._q(
                "SELECT tbl_name FROM sqlite_master WHERE type='index' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )
        probes = [
            ("SELECT COUNT(*) FROM \"{t}\" WHERE rowid > 0",       "Full COUNT scan"),
            ("SELECT * FROM \"{t}\" ORDER BY rowid DESC LIMIT 100", "Full ORDER BY scan"),
        ]
        for t in tables:
            tname = t["name"]
            count = self._q(f'SELECT COUNT(*) AS cnt FROM "{tname}"')[0]["cnt"]
            if count < 5000:
                continue
            for tmpl, desc in probes:
                sql = tmpl.format(t=tname)
                start = time.perf_counter()
                try:
                    self.conn.execute(sql).fetchall()
                except Exception:
                    continue
                elapsed_ms = (time.perf_counter() - start) * 1000
                if elapsed_ms >= 1:
                    results.append({
                        "query":   f"{desc} on {tname}",
                        "mean_ms": round(elapsed_ms, 2),
                        "calls":   1,
                        "rows":    count,
                        "table":   tname,
                        "indexed": tname in indexed,
                    })
            if len(results) >= limit:
                break
        return sorted(results, key=lambda r: r["mean_ms"], reverse=True)[:limit]

    # ── Missing Indexes ───────────────────────────────────────────────────────
    def missing_indexes(self) -> list[dict]:
        tables = self._q("SELECT name FROM sqlite_master WHERE type='table'")
        indexed = set(
            r["tbl_name"]
            for r in self._q(
                "SELECT tbl_name FROM sqlite_master WHERE type='index' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )
        issues = []
        for t in tables:
            tname = t["name"]
            if tname in indexed:
                continue
            count = self._q(f'SELECT COUNT(*) AS cnt FROM "{tname}"')[0]["cnt"]
            if count < 1000:
                continue
            issues.append({
                "table_name":  tname,
                "seq_scan":    count,
                "scan_detail": f"SCAN {tname} (~{count:,} rows, no non-PK index)",
            })
        return issues

    # ── Table Bloat ───────────────────────────────────────────────────────────
    def table_bloat(self) -> list[dict]:
        page_size  = self._q("PRAGMA page_size")[0]["page_size"]
        page_count = self._q("PRAGMA page_count")[0]["page_count"]
        freelist   = self._q("PRAGMA freelist_count")[0]["freelist_count"]
        file_size  = os.path.getsize(self.path)

        wasted_mb  = round(freelist * page_size / 1024 / 1024, 3)
        total_mb   = round(file_size / 1024 / 1024, 3)
        wasted_pct = round(freelist * 100.0 / max(page_count, 1), 2)

        rows = []
        # Whole-DB bloat summary
        rows.append({
            "table_name": "__database__",
            "dead_pct":   wasted_pct,
            "n_dead_tup": freelist,
            "n_live_tup": page_count - freelist,
            "wasted_mb":  wasted_mb,
            "total_mb":   total_mb,
        })
        # Per-table size via dbstat
        tables = self._q("SELECT name FROM sqlite_master WHERE type='table'")
        for t in tables:
            tname = t["name"]
            try:
                count = self._q(f'SELECT COUNT(*) AS cnt FROM "{tname}"')[0]["cnt"]
                size_pages = self._q("SELECT COUNT(*) AS p FROM dbstat WHERE name=?", (tname,))
                size_mb = round(size_pages[0]["p"] * page_size / 1024 / 1024, 3) if size_pages else 0
                if size_mb > 0.5:
                    rows.append({
                        "table_name": tname,
                        "dead_pct":   0,
                        "n_dead_tup": 0,
                        "n_live_tup": count,
                        "size_mb":    size_mb,
                    })
            except Exception:
                pass
        return rows

    # ── Resource Usage ────────────────────────────────────────────────────────
    def resource_usage(self) -> dict:
        page_size  = self._q("PRAGMA page_size")[0]["page_size"]
        cache_size = self._q("PRAGMA cache_size")[0]["cache_size"]
        freelist   = self._q("PRAGMA freelist_count")[0]["freelist_count"]
        journal    = self._q("PRAGMA journal_mode")[0]["journal_mode"]
        file_size  = os.path.getsize(self.path)

        cache_kb   = abs(cache_size) if cache_size < 0 else cache_size * page_size // 1024
        db_kb      = file_size / 1024
        coverage_pct = round(min(cache_kb / max(db_kb, 1) * 100, 100), 1)

        return {
            "cache_hit": [{"cache_hit_pct": coverage_pct}],
            "locks":     [{"blocked_queries": 0}],
            "sqlite": {
                "file_size_mb":        round(file_size / 1024 / 1024, 3),
                "page_size_bytes":     page_size,
                "freelist_pages":      freelist,
                "cache_size_kb":       cache_kb,
                "journal_mode":        journal,
                "cache_covers_db_pct": coverage_pct,
            },
        }

    # ── Index Usage ───────────────────────────────────────────────────────────
    def index_usage(self) -> list[dict]:
        indexes = self._q(
            "SELECT name, tbl_name FROM sqlite_master "
            "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
        results = []
        for idx in indexes:
            iname = idx["name"]
            tname = idx["tbl_name"]
            try:
                cols = self._q(f"PRAGMA index_info({iname})")
                col_names = [c["name"] for c in cols]
            except Exception:
                col_names = []
            count = self._q(f'SELECT COUNT(*) AS cnt FROM "{tname}"')[0]["cnt"]
            t0 = time.perf_counter()
            try:
                self.conn.execute(f'SELECT COUNT(*) FROM "{tname}" INDEXED BY "{iname}"').fetchone()
                idx_ms = round((time.perf_counter() - t0) * 1000, 2)
            except Exception:
                idx_ms = None
            results.append({
                "index_name": iname,
                "table_name": tname,
                "columns":    ", ".join(col_names),
                "table_rows": count,
                "scan_ms":    idx_ms,
            })
        return results

    # ── Long Running ──────────────────────────────────────────────────────────
    def long_running(self, threshold_sec: int = 5) -> list[dict]:
        results = []
        tables = self._q("SELECT name FROM sqlite_master WHERE type='table'")
        expensive_patterns = [
            ('SELECT * FROM "{t}" WHERE typeof(rowid) != \'integer\'',
             "Type-cast full scan"),
            ('SELECT a.rowid FROM "{t}" a, "{t}" b WHERE a.rowid != b.rowid LIMIT 1000',
             "Cartesian join scan"),
        ]
        for t in tables:
            tname = t["name"]
            count = self._q(f'SELECT COUNT(*) AS cnt FROM "{tname}"')[0]["cnt"]
            if count < 10_000:
                continue
            for tmpl, desc in expensive_patterns:
                sql = tmpl.format(t=tname)
                start = time.perf_counter()
                try:
                    self.conn.execute(sql).fetchall()
                except Exception:
                    continue
                elapsed_ms = (time.perf_counter() - start) * 1000
                if elapsed_ms >= 5:
                    results.append({
                        "query":        f"{desc} on {tname}",
                        "duration":     f"{elapsed_ms:.0f} ms",
                        "elapsed_ms":   elapsed_ms,
                        "rows_scanned": count,
                    })
        return sorted(results, key=lambda r: r["elapsed_ms"], reverse=True)[:10]

    def close(self):
        self.conn.close()