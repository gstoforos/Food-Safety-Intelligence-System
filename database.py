"""
FSIS Agent – Database layer (SQLite)
"""
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS recalls (
            id                  TEXT PRIMARY KEY,   -- recall_number
            recall_date         TEXT,               -- recall_initiation_date (ISO)
            firm                TEXT,
            product             TEXT,
            reason              TEXT,
            classification      TEXT,               -- Class I / II / III
            status              TEXT,               -- Ongoing / Completed / Terminated
            distribution        TEXT,
            quantity            TEXT,
            city                TEXT,
            state               TEXT,
            country             TEXT,
            -- AI-enriched fields
            pathogen            TEXT,               -- primary pathogen detected
            pathogen_confidence TEXT,               -- HIGH / MEDIUM / LOW
            risk_summary        TEXT,               -- 1-sentence AI summary
            affected_population TEXT,               -- e.g. "immunocompromised, elderly"
            raw_json            TEXT,               -- full openFDA record
            enriched            INTEGER DEFAULT 0,  -- 0 = raw, 1 = Gemini enriched
            inserted_at         TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS run_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at      TEXT DEFAULT (datetime('now')),
            source      TEXT,
            fetched     INTEGER,
            new_records INTEGER,
            status      TEXT,
            error       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_recalls_date       ON recalls(recall_date);
        CREATE INDEX IF NOT EXISTS idx_recalls_pathogen   ON recalls(pathogen);
        CREATE INDEX IF NOT EXISTS idx_recalls_class      ON recalls(classification);
        CREATE INDEX IF NOT EXISTS idx_recalls_enriched   ON recalls(enriched);
    """)
    conn.commit()
    conn.close()


def upsert_recall(rec: dict) -> bool:
    """Insert or ignore (returns True if new record)."""
    conn = get_conn()
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO recalls
            (id, recall_date, firm, product, reason, classification,
             status, distribution, quantity, city, state, country, raw_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rec["id"],
            rec.get("recall_date", ""),
            rec.get("firm", ""),
            rec.get("product", ""),
            rec.get("reason", ""),
            rec.get("classification", ""),
            rec.get("status", ""),
            rec.get("distribution", ""),
            rec.get("quantity", ""),
            rec.get("city", ""),
            rec.get("state", ""),
            rec.get("country", ""),
            json.dumps(rec.get("raw", {})),
        ),
    )
    new = cur.rowcount > 0
    conn.commit()
    conn.close()
    return new


def update_enrichment(recall_id: str, pathogen: str, confidence: str,
                      risk_summary: str, affected_population: str):
    conn = get_conn()
    conn.execute(
        """
        UPDATE recalls SET
            pathogen            = ?,
            pathogen_confidence = ?,
            risk_summary        = ?,
            affected_population = ?,
            enriched            = 1
        WHERE id = ?
        """,
        (pathogen, confidence, risk_summary, affected_population, recall_id),
    )
    conn.commit()
    conn.close()


def get_unenriched(limit: int = 50) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM recalls WHERE enriched=0 ORDER BY recall_date DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_recalls(days: int = 30) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT * FROM recalls
        WHERE recall_date >= date('now', ? || ' days')
        ORDER BY recall_date DESC
        """,
        (f"-{days}",),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats(days: int = 30) -> dict:
    conn = get_conn()

    total = conn.execute(
        "SELECT COUNT(*) FROM recalls WHERE recall_date >= date('now', ? || ' days')",
        (f"-{days}",),
    ).fetchone()[0]

    by_class = conn.execute(
        """
        SELECT classification, COUNT(*) as cnt
        FROM recalls
        WHERE recall_date >= date('now', ? || ' days')
        GROUP BY classification ORDER BY cnt DESC
        """,
        (f"-{days}",),
    ).fetchall()

    by_pathogen = conn.execute(
        """
        SELECT COALESCE(pathogen,'Unclassified') as pathogen, COUNT(*) as cnt
        FROM recalls
        WHERE recall_date >= date('now', ? || ' days')
        GROUP BY pathogen ORDER BY cnt DESC LIMIT 10
        """,
        (f"-{days}",),
    ).fetchall()

    by_month = conn.execute(
        """
        SELECT substr(recall_date,1,7) as month, COUNT(*) as cnt
        FROM recalls
        WHERE recall_date >= date('now','-365 days')
        GROUP BY month ORDER BY month
        """,
    ).fetchall()

    conn.close()
    return {
        "total":       total,
        "by_class":    [dict(r) for r in by_class],
        "by_pathogen": [dict(r) for r in by_pathogen],
        "by_month":    [dict(r) for r in by_month],
    }


def log_run(source: str, fetched: int, new_records: int,
            status: str = "OK", error: str = ""):
    conn = get_conn()
    conn.execute(
        "INSERT INTO run_log (source,fetched,new_records,status,error) VALUES (?,?,?,?,?)",
        (source, fetched, new_records, status, error),
    )
    conn.commit()
    conn.close()
