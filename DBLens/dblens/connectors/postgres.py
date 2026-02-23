"""PostgreSQL connector for DBLens."""
from __future__ import annotations
import re
from typing import Any


class PostgresConnector:
    """Connects to PostgreSQL and fetches diagnostic data."""

    def __init__(self, dsn: str):
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self._cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _q(self, sql: str, params=None) -> list[dict]:
        self._cursor.execute(sql, params)
        return [dict(r) for r in self._cursor.fetchall()]

    # ── Slow Queries ────────────────────────────────────────────────────────
    def slow_queries(self, limit: int = 20) -> list[dict]:
        try:
            return self._q(
                """
                SELECT
                    query,
                    calls,
                    round(total_exec_time::numeric, 2)   AS total_ms,
                    round(mean_exec_time::numeric,  2)   AS mean_ms,
                    round(stddev_exec_time::numeric, 2)  AS stddev_ms,
                    round((mean_exec_time * calls)::numeric / NULLIF(SUM(total_exec_time) OVER(), 0) * 100, 2) AS pct_total,
                    rows
                FROM pg_stat_statements
                ORDER BY mean_exec_time DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            return []

    # ── Missing Indexes ──────────────────────────────────────────────────────
    def missing_indexes(self) -> list[dict]:
        return self._q(
            """
            SELECT
                schemaname || '.' || relname                     AS table_name,
                seq_scan,
                seq_tup_read,
                idx_scan,
                round(seq_tup_read::numeric / NULLIF(seq_scan,0), 0) AS avg_rows_per_seq_scan
            FROM pg_stat_user_tables
            WHERE seq_scan > 50
              AND (idx_scan IS NULL OR seq_scan > idx_scan * 3)
            ORDER BY seq_tup_read DESC
            LIMIT 20
            """
        )

    # ── Table Bloat ──────────────────────────────────────────────────────────
    def table_bloat(self) -> list[dict]:
        return self._q(
            """
            SELECT
                schemaname || '.' || relname AS table_name,
                n_dead_tup,
                n_live_tup,
                CASE WHEN n_live_tup > 0
                     THEN round(n_dead_tup * 100.0 / n_live_tup, 2)
                     ELSE 0 END             AS dead_pct,
                last_vacuum,
                last_autovacuum,
                last_analyze
            FROM pg_stat_user_tables
            WHERE n_dead_tup > 1000
            ORDER BY dead_pct DESC
            LIMIT 20
            """
        )

    # ── Resource Usage ───────────────────────────────────────────────────────
    def resource_usage(self) -> dict:
        connections = self._q(
            """
            SELECT state, count(*) AS cnt
            FROM pg_stat_activity
            GROUP BY state
            """
        )
        db_size = self._q(
            """
            SELECT datname,
                   pg_size_pretty(pg_database_size(datname)) AS size
            FROM pg_stat_database
            WHERE datname = current_database()
            """
        )
        cache_hit = self._q(
            """
            SELECT
                round(sum(heap_blks_hit)  * 100.0 /
                      NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) AS cache_hit_pct
            FROM pg_statio_user_tables
            """
        )
        locks = self._q(
            """
            SELECT count(*) AS blocked_queries
            FROM pg_stat_activity
            WHERE wait_event_type = 'Lock'
            """
        )
        return {
            "connections": connections,
            "db_size": db_size,
            "cache_hit": cache_hit,
            "locks": locks,
        }

    # ── Index Usage ──────────────────────────────────────────────────────────
    def index_usage(self) -> list[dict]:
        return self._q(
            """
            SELECT
                schemaname || '.' || relname AS table_name,
                indexrelname                 AS index_name,
                idx_scan,
                pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
            FROM pg_stat_user_indexes
            ORDER BY idx_scan ASC
            LIMIT 20
            """
        )

    # ── Long Running Queries ─────────────────────────────────────────────────
    def long_running(self, threshold_sec: int = 5) -> list[dict]:
        return self._q(
            """
            SELECT
                pid,
                now() - query_start      AS duration,
                state,
                left(query, 120)         AS query
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query_start IS NOT NULL
              AND now() - query_start > interval '%s seconds'
            ORDER BY duration DESC
            """ % threshold_sec
        )

    def close(self):
        self.conn.close()
