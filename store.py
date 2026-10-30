"""SQLite persistence: tasks, message idempotency records, decision cache, executions, debug log."""
import json
import sqlite3
import threading
import time


class Store:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        with self.lock:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
            CREATE TABLE IF NOT EXISTS tasks(
                id TEXT PRIMARY KEY, principal TEXT NOT NULL, state TEXT NOT NULL,
                created REAL NOT NULL, updated REAL NOT NULL, json TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS tasks_principal ON tasks(principal, created);
            CREATE TABLE IF NOT EXISTS messages(
                principal TEXT NOT NULL, message_id TEXT NOT NULL, hash TEXT NOT NULL, task_id TEXT NOT NULL,
                PRIMARY KEY(principal, message_id));
            CREATE TABLE IF NOT EXISTS decisions(key TEXT PRIMARY KEY, json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS executions(
                action_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, principal TEXT NOT NULL, created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS debug(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, kind TEXT, json TEXT);
            """)

    # ---- transactions ----
    def tx(self):
        return _Tx(self)

    # ---- tasks ----
    def get_task(self, task_id):
        with self.lock:
            r = self.db.execute("SELECT principal, json FROM tasks WHERE id=?", (task_id,)).fetchone()
        return (r[0], json.loads(r[1])) if r else None

    def put_task(self, task_id, principal, state, task, created=None):
        now = time.time()
        with self.lock:
            self.db.execute(
                "INSERT INTO tasks(id,principal,state,created,updated,json) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET state=excluded.state, updated=excluded.updated, json=excluded.json",
                (task_id, principal, state, created or now, now, json.dumps(task, separators=(",", ":"), ensure_ascii=False)))

    def list_tasks(self, principal):
        with self.lock:
            rows = self.db.execute(
                "SELECT json FROM tasks WHERE principal=? ORDER BY created DESC, rowid DESC", (principal,)).fetchall()
        return [json.loads(r[0]) for r in rows]

    # ---- messages (idempotency) ----
    def get_message(self, principal, message_id):
        with self.lock:
            r = self.db.execute("SELECT hash, task_id FROM messages WHERE principal=? AND message_id=?",
                                (principal, message_id)).fetchone()
        return {"hash": r[0], "task_id": r[1]} if r else None

    def put_message(self, principal, message_id, h, task_id):
        with self.lock:
            self.db.execute("INSERT OR IGNORE INTO messages VALUES(?,?,?,?)", (principal, message_id, h, task_id))

    # ---- decision cache ----
    def get_decision(self, key):
        with self.lock:
            r = self.db.execute("SELECT json FROM decisions WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else None

    def put_decision(self, key, d):
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO decisions VALUES(?,?)", (key, json.dumps(d, ensure_ascii=False)))

    # ---- executions ----
    def record_execution(self, action_id, task_id, principal):
        with self.lock:
            cur = self.db.execute("INSERT OR IGNORE INTO executions VALUES(?,?,?,?)",
                                  (action_id, task_id, principal, time.time()))
            return cur.rowcount == 1

    # ---- debug ----
    def debug(self, kind, payload, keep=60):
        with self.lock:
            self.db.execute("INSERT INTO debug(ts,kind,json) VALUES(?,?,?)",
                            (time.time(), kind, json.dumps(payload, ensure_ascii=False, default=str)))
            self.db.execute("DELETE FROM debug WHERE id NOT IN (SELECT id FROM debug ORDER BY id DESC LIMIT ?)", (keep,))

    def debug_list(self, kind=None, limit=20):
        q, a = "SELECT id, ts, kind, length(json) FROM debug", []
        if kind:
            q += " WHERE kind=?"; a.append(kind)
        q += " ORDER BY id DESC LIMIT ?"; a.append(limit)
        with self.lock:
            return self.db.execute(q, a).fetchall()

    def debug_get(self, did):
        with self.lock:
            r = self.db.execute("SELECT json FROM debug WHERE id=?", (did,)).fetchone()
        return json.loads(r[0]) if r else None


class _Tx:
    """BEGIN IMMEDIATE ... COMMIT so a multi-row change lands atomically (all or nothing)."""
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.s.lock.acquire()
        self.s.db.execute("BEGIN IMMEDIATE")
        return self.s

    def __exit__(self, et, ev, tb):
        try:
            self.s.db.execute("ROLLBACK" if et else "COMMIT")
        finally:
            self.s.lock.release()
        return False
