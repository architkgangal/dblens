"""MongoDB connector for DBLens."""
from __future__ import annotations


class MongoConnector:
    """Connects to MongoDB and fetches diagnostic data."""

    def __init__(self, uri: str, database: str):
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ImportError("pymongo not installed. Run: pip install pymongo")
        from pymongo import MongoClient
        self.client = MongoClient(uri)
        self.db = self.client[database]

    # ── Slow Queries (currentOp + system.profile) ────────────────────────────
    def slow_queries(self, limit: int = 20) -> list[dict]:
        try:
            profiling = list(
                self.db["system.profile"]
                .find({"op": {"$ne": "getmore"}})
                .sort("millis", -1)
                .limit(limit)
            )
            results = []
            for p in profiling:
                results.append({
                    "ns":       p.get("ns", ""),
                    "op":       p.get("op", ""),
                    "millis":   p.get("millis", 0),
                    "query":    str(p.get("command", p.get("query", "")))[:120],
                    "docsExamined": p.get("docsExamined", 0),
                    "keysExamined": p.get("keysExamined", 0),
                    "nreturned":    p.get("nreturned", 0),
                })
            return results
        except Exception as e:
            return [{"error": str(e)}]

    # ── Missing Indexes (collections with high scans) ────────────────────────
    def missing_indexes(self) -> list[dict]:
        issues = []
        for cname in self.db.list_collection_names():
            try:
                stats = self.db.command("collStats", cname)
                total_docs = stats.get("count", 0)
                indexes = list(self.db[cname].index_information().keys())
                if total_docs > 1000 and len(indexes) <= 1:
                    issues.append({
                        "collection": cname,
                        "documents": total_docs,
                        "indexes": indexes,
                        "note": "Large collection with only _id index",
                    })
            except Exception:
                pass
        return issues

    # ── Collection Stats / Bloat ─────────────────────────────────────────────
    def table_bloat(self) -> list[dict]:
        results = []
        for cname in self.db.list_collection_names():
            try:
                stats = self.db.command("collStats", cname)
                results.append({
                    "collection":    cname,
                    "documents":     stats.get("count", 0),
                    "size_mb":       round(stats.get("size", 0) / 1024 / 1024, 3),
                    "storage_mb":    round(stats.get("storageSize", 0) / 1024 / 1024, 3),
                    "index_size_mb": round(stats.get("totalIndexSize", 0) / 1024 / 1024, 3),
                    "avg_obj_bytes": stats.get("avgObjSize", 0),
                })
            except Exception:
                pass
        return sorted(results, key=lambda x: x.get("storage_mb", 0), reverse=True)

    # ── Resource Usage ────────────────────────────────────────────────────────
    def resource_usage(self) -> dict:
        server_status = self.db.command("serverStatus")
        return {
            "connections": server_status.get("connections", {}),
            "opcounters":  server_status.get("opcounters", {}),
            "memory_mb": {
                "resident": server_status.get("mem", {}).get("resident", 0),
                "virtual":  server_status.get("mem", {}).get("virtual", 0),
            },
            "uptime_sec":  server_status.get("uptime", 0),
        }

    # ── Long Running Operations ───────────────────────────────────────────────
    def long_running(self, threshold_sec: int = 5) -> list[dict]:
        try:
            ops = self.db.command("currentOp", {"active": True, "secs_running": {"$gte": threshold_sec}})
            results = []
            for op in ops.get("inprog", []):
                results.append({
                    "opid":      op.get("opid", ""),
                    "op":        op.get("op", ""),
                    "ns":        op.get("ns", ""),
                    "secs_running": op.get("secs_running", 0),
                    "query":     str(op.get("command", ""))[:120],
                })
            return results
        except Exception as e:
            return [{"error": str(e)}]

    def close(self):
        self.client.close()
