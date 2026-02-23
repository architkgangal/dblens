"""Snowflake connector for DBLens."""
from __future__ import annotations


class SnowflakeConnector:
    """Connects to Snowflake and fetches diagnostic data."""

    def __init__(self, account: str, user: str, password: str, database: str,
                 warehouse: str, schema: str = "PUBLIC"):
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError("snowflake-connector-python not installed. "
                              "Run: pip install snowflake-connector-python")
        import snowflake.connector
        self.conn = snowflake.connector.connect(
            account=account, user=user, password=password,
            database=database, warehouse=warehouse, schema=schema
        )

    def _q(self, sql: str) -> list[dict]:
        cur = self.conn.cursor(self.conn.cursor().__class__)
        cur = self.conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── Slow Queries ─────────────────────────────────────────────────────────
    def slow_queries(self, limit: int = 20) -> list[dict]:
        return self._q(
            f"""
            SELECT query_text,
                   execution_time / 1000            AS exec_sec,
                   bytes_scanned / 1024 / 1024      AS mb_scanned,
                   rows_produced,
                   partitions_total,
                   partitions_scanned,
                   compilation_time / 1000          AS compile_sec,
                   queued_overload_time / 1000       AS queued_sec,
                   warehouse_name,
                   user_name,
                   start_time
            FROM snowflake.account_usage.query_history
            WHERE execution_status = 'SUCCESS'
              AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP)
            ORDER BY execution_time DESC
            LIMIT {limit}
            """
        )

    # ── Partition Pruning Issues (missing clustering) ────────────────────────
    def missing_indexes(self) -> list[dict]:
        return self._q(
            """
            SELECT query_text,
                   partitions_scanned,
                   partitions_total,
                   ROUND(partitions_scanned * 100.0 / NULLIF(partitions_total, 0), 2) AS pct_scanned,
                   bytes_scanned / 1024 / 1024 AS mb_scanned
            FROM snowflake.account_usage.query_history
            WHERE partitions_total > 10
              AND partitions_scanned * 1.0 / NULLIF(partitions_total, 0) > 0.8
              AND start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP)
            ORDER BY mb_scanned DESC
            LIMIT 20
            """
        )

    # ── Table / Storage Stats ─────────────────────────────────────────────────
    def table_bloat(self) -> list[dict]:
        return self._q(
            """
            SELECT table_schema,
                   table_name,
                   row_count,
                   ROUND(bytes / 1024 / 1024, 2)              AS size_mb,
                   ROUND(bytes_compressed / 1024 / 1024, 2)   AS compressed_mb,
                   clustering_key
            FROM information_schema.tables
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
            ORDER BY bytes DESC
            LIMIT 30
            """
        )

    # ── Resource Usage ────────────────────────────────────────────────────────
    def resource_usage(self) -> dict:
        credits = self._q(
            """
            SELECT warehouse_name,
                   SUM(credits_used) AS total_credits,
                   SUM(credits_used_compute) AS compute_credits
            FROM snowflake.account_usage.warehouse_metering_history
            WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP)
            GROUP BY 1
            ORDER BY total_credits DESC
            LIMIT 10
            """
        )
        failed = self._q(
            """
            SELECT COUNT(*) AS failed_queries
            FROM snowflake.account_usage.query_history
            WHERE execution_status = 'FAIL'
              AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP)
            """
        )
        return {"warehouse_credits_7d": credits, "failed_queries_24h": failed}

    # ── Long Running Queries ──────────────────────────────────────────────────
    def long_running(self, threshold_sec: int = 30) -> list[dict]:
        return self._q(
            f"""
            SELECT query_id,
                   query_text,
                   execution_time / 1000 AS exec_sec,
                   user_name,
                   warehouse_name,
                   start_time
            FROM snowflake.account_usage.query_history
            WHERE execution_time / 1000 >= {threshold_sec}
              AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP)
            ORDER BY execution_time DESC
            LIMIT 20
            """
        )

    def close(self):
        self.conn.close()
