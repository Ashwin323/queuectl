import sqlite3
import threading
from datetime import datetime

DB_FILE = "queue.db"
_lock = threading.Lock()


def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock:
        conn = get_connection()
        cursor = conn.cursor()

        # Table for all jobs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        # Dead Letter Queue table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id TEXT PRIMARY KEY,
                command TEXT,
                reason TEXT,
                failed_at TEXT
            )
        """)

        # Configuration table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()
        conn.close()


def insert_job(job):
    with _lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            job["id"],
            job["command"],
            job.get("state", "pending"),
            job.get("attempts", 0),
            job.get("max_retries", 3),
            job.get("created_at", datetime.utcnow().isoformat()),
            job.get("updated_at", datetime.utcnow().isoformat())
        ))
        conn.commit()
        conn.close()


def update_job_state(job_id, state, attempts=None):
    with _lock:
        conn = get_connection()
        cursor = conn.cursor()
        query = "UPDATE jobs SET state = ?, updated_at = ?"
        params = [state, datetime.utcnow().isoformat()]

        if attempts is not None:
            query += ", attempts = ?"
            params.append(attempts)

        query += " WHERE id = ?"
        params.append(job_id)
        cursor.execute(query, tuple(params))
        conn.commit()
        conn.close()


def move_to_dlq(job_id, command, reason):
    with _lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO dlq (id, command, reason, failed_at)
            VALUES (?, ?, ?, ?)
        """, (job_id, command, reason, datetime.utcnow().isoformat()))
        cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
        conn.close()


def fetch_jobs_by_state(state):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE state = ?", (state,))
    jobs = cursor.fetchall()
    conn.close()
    return jobs


def get_job(job_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    job = cursor.fetchone()
    conn.close()
    return job


def fetch_all_dlq():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dlq")
    dlq_jobs = cursor.fetchall()
    conn.close()
    return dlq_jobs


def get_config(key, default=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else default


def set_config(key, value):
    with _lock:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()
        conn.close()
