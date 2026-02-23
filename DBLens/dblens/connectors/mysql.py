"""MySQL / MariaDB connector for DBLens."""
from __future__ import annotations


class MySQLConnector:
    """Connects to MySQL/MariaDB and fetches diagnostic data."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("mysql-connector-python not installed. Run: pip install mysql-connector-python")
        self.conn = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=database
        )

    def _q(self, sql: str, params=None) -> list[dict]:
        cur = self.conn.cursor(dictionary=True)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows

    # ── Slow Queries ─────────────────────────────────────────────────────────
    def slow_queries(self, limit: int = 20) -> list[dict]:
        return self._q(
            """
            SELECT DIGEST_TEXT        AS query,
                   COUNT_STAR         AS calls,
                   ROUND(SUM_TIMER_WAIT / 1e12, 4)  AS total_sec,
                   ROUND(AVG_TIMER_WAIT / 1e12, 4)  AS mean_sec,
                   SUM_ROWS_EXAMINED  AS rows_examined,
                   SUM_ROWS_SENT      AS rows_sent
            FROM performance_schema.events_statements_summary_by_digest
            ORDER BY AVG_TIMER_WAIT DESC
            LIMIT %s
            """,
            (limit,),
        )

    # ── Missing Indexes ───────────────────────────────────────────────────────
    def missing_indexes(self) -> list[dict]:
        return self._q(
            """
            SELECT object_schema AS schema_name,
                   object_name   AS table_name,
                   count_read    AS full_scans
            FROM performance_schema.table_io_waits_summary_by_table
            WHERE count_read > 1000
            ORDER BY count_read DESC
            LIMIT 20
            """
        )

    # ── Table Sizes ───────────────────────────────────────────────────────────
    def table_bloat(self) -> list[dict]:
        return self._q(
            """
            SELECT table_schema,
                   table_name,
                   ROUND(data_length / 1024 / 1024, 2)     AS data_mb,
                   ROUND(index_length / 1024 / 1024, 2)    AS index_mb,
                   ROUND(data_free  / 1024 / 1024, 2)      AS free_mb,
                   table_rows
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
            ORDER BY data_free DESC
            LIMIT 20
            """
        )

    # ── Resource Usage ────────────────────────────────────────────────────────
    def resource_usage(self) -> dict:
        status = self._q("SHOW GLOBAL STATUS WHERE Variable_name IN "
                         "('Threads_connected','Threads_running','Slow_queries',"
                         "'Questions','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_reads')")
        variables = self._q("SHOW VARIABLES WHERE Variable_name IN "
                            "('max_connections','innodb_buffer_pool_size','query_cache_size')")
        return {"status": status, "variables": variables}

    # ── Long Running Queries ──────────────────────────────────────────────────
    def long_running(self, threshold_sec: int = 5) -> list[dict]:
        return self._q(
            """
            SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 120) AS query
            FROM information_schema.PROCESSLIST
            WHERE COMMAND != 'Sleep'
              AND TIME >= %s
            ORDER BY TIME DESC
            """,
            (threshold_sec,),
        )

    def close(self):
        self.conn.close()
