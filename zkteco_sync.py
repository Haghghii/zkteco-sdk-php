# zkteco_sync.py
import os
import sys
import time
import sqlite3
import logging
from typing import Iterable, List, Tuple, Optional, Dict, Any
from contextlib import contextmanager
from datetime import datetime

from dotenv import load_dotenv
import requests
from zk import ZK, const  # pip install zk

# ---------------- Config ----------------
load_dotenv()

ZK_IP               = os.getenv("ZK_IP", "46.100.10.217")
ZK_PORT             = int(os.getenv("ZK_PORT", "4370"))
ZK_TIMEOUT          = int(os.getenv("ZK_TIMEOUT", "12"))          # seconds
API_URL             = os.getenv("API_URL", "https://portal.sobhe-roshan.ir/api/v1/ElectronicAttendance")
API_PASS            = os.getenv("API_PASS", "1234")               # '' if none
CLEAR_DEVICE_LOGS   = os.getenv("CLEAR_DEVICE_LOGS", "false").lower() == "true"
GET_RETRIES         = int(os.getenv("GET_RETRIES", "3"))
RECONNECT_DELAY_MS  = int(os.getenv("RECONNECT_DELAY_MS", "1200"))
SQLITE_PATH         = os.getenv("SQLITE_PATH", "attendance.db")
TZ                  = os.getenv("TZ", "Asia/Tehran")
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO").upper()
BATCH_LIMIT         = int(os.getenv("BATCH_LIMIT", "500"))
HTTP_TIMEOUT        = int(os.getenv("HTTP_TIMEOUT", "15"))

# ---------------- Logging ----------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
try:
    import pytz  # optional
    os.environ["TZ"] = TZ
except Exception:
    pass

# ---------------- SQLite ----------------
def connect_db(path: str) -> sqlite3.Connection:
    need_init = not os.path.exists(path)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)  # autocommit off by default with BEGIN
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    if need_init:
        logging.info("Creating SQLite schema at %s", path)
    init_db(conn)
    return conn

def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS attendance_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id     TEXT NOT NULL,
        record_time TEXT NOT NULL,
        server_id   TEXT,
        sent_at     TEXT,
        created_at  TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(user_id, record_time)
    );
    """)
    conn.commit()

def insert_attendance_bulk(conn: sqlite3.Connection, rows: List[Tuple[str, str]]) -> int:
    """
    rows: list of (user_id, record_time_iso)
    INSERT OR IGNORE to avoid duplicates (UNIQUE(user_id, record_time))
    """
    cur = conn.cursor()
    inserted = 0
    cur.execute("BEGIN")
    try:
        for user_id, record_time in rows:
            if not user_id or not record_time:
                continue
            cur.execute(
                "INSERT OR IGNORE INTO attendance_logs (user_id, record_time) VALUES (?, ?)",
                (user_id, record_time)
            )
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.exception("DB bulk insert failed: %s", e)
        raise
    return inserted

def fetch_unsent(conn: sqlite3.Connection, limit: int = BATCH_LIMIT) -> List[Tuple[int, str, str]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, user_id, record_time
        FROM attendance_logs
        WHERE server_id IS NULL
        ORDER BY record_time ASC
        LIMIT ?
    """, (limit,))
    return cur.fetchall()

def mark_sent(conn: sqlite3.Connection, row_id: int, server_id: str):
    conn.execute("""
        UPDATE attendance_logs
        SET server_id = ?, sent_at = datetime('now'), updated_at = datetime('now')
        WHERE id = ?
    """, (server_id, row_id))
    conn.commit()

# ---------------- ZK helpers ----------------
@contextmanager
def zk_connection(ip: str, port: int, timeout: int):
    zk = ZK(ip=ip, port=port, timeout=timeout, password=0, force_udp=False, ommit_ping=False)
    conn = None
    try:
        conn = zk.connect()
        if conn:
            logging.info("Connected to ZKTeco %s:%s", ip, port)
        yield conn
    finally:
        try:
            if conn:
                conn.disconnect()
        except Exception:
            pass
        logging.info("Disconnected")

def to_iso(dt) -> Optional[str]:
    """
    Convert supported types (datetime/str/int/float) to ISO-8601 string.
    """
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.isoformat()
    if isinstance(dt, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(dt)).isoformat() + "Z"
        except Exception:
            return None
    if isinstance(dt, str):
        s = dt.strip()
        # Try ISO parse
        try:
            # handle trailing Z
            if s.endswith("Z"):
                return datetime.fromisoformat(s.replace("Z", "+00:00")).isoformat()
            return datetime.fromisoformat(s).isoformat()
        except Exception:
            # Fallback: accept as-is
            return s
    return str(dt)

def map_attendance_item(item) -> Optional[Tuple[str, str]]:
    """
    Try to normalize a single attendance record to (user_id, record_time_iso)
    Common keys: user_id/userid/uid/pin/id, timestamp/time/attTime/checkTime
    """
    user_id = None
    record_time = None

    # user id candidates
    for key in ("user_id", "userid", "uid", "user", "enrollNumber", "enroll_number", "pin", "PIN", "id", "UserID"):
        if hasattr(item, key):
            v = getattr(item, key)
            if v is not None:
                user_id = str(v)
                break
        elif isinstance(item, dict) and key in item and item[key] is not None:
            user_id = str(item[key])
            break

    # time candidates
    for key in ("timestamp", "time", "record_time", "recordtime", "attTime", "checkTime"):
        if hasattr(item, key):
            record_time = getattr(item, key)
            break
        elif isinstance(item, dict) and key in item:
            record_time = item[key]
            break

    iso = to_iso(record_time)
    if not user_id or not iso:
        return None
    return user_id, iso

def fetch_logs_once(ip: str, port: int, timeout: int) -> List[Tuple[str, str]]:
    with zk_connection(ip, port, timeout) as conn:
        if not conn:
            logging.warning("Connection failed")
            return []

        try:
            conn.disable_device()
            time.sleep(0.2)
        except Exception:
            pass

        raw_items = []
        try:
            raw_items = conn.get_attendance() or []
        except Exception as e:
            logging.warning("get_attendance error: %s", e)
            raw_items = []

        try:
            conn.enable_device()
        except Exception:
            pass

        results: List[Tuple[str, str]] = []
        for it in raw_items:
            mapped = map_attendance_item(it)
            if mapped:
                results.append(mapped)
        return results

def fetch_logs_with_retries(ip: str, port: int, timeout: int, retries: int, delay_ms: int) -> List[Tuple[str, str]]:
    for i in range(retries):
        logs = fetch_logs_once(ip, port, timeout)
        if logs:
            return logs
        logging.info("No logs returned; retrying (%d/%d)...", i + 1, retries)
        time.sleep(delay_ms / 1000.0)
    return []

def clear_device_logs():
    with zk_connection(ZK_IP, ZK_PORT, ZK_TIMEOUT) as conn:
        if not conn:
            logging.error("Cannot connect to clear logs")
            return
        try:
            if hasattr(conn, "clear_attendance"):
                conn.clear_attendance()
            elif hasattr(conn, "clear_attendance_log"):
                conn.clear_attendance_log()
            else:
                logging.error("No clear_attendance method available")
                return
            logging.info("Device logs cleared.")
        except Exception as e:
            logging.error("Error clearing device logs: %s", e)

# ---------------- API ----------------
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "ZKSync/1.0"
}

def post_attendance(payload: Dict[str, Any],
                    timeout: int = HTTP_TIMEOUT,
                    max_retries: int = 3) -> Optional[str]:
    """
    Send one record to API. Return server_id string on success, None otherwise.
    Retries with simple backoff.
    """
    with requests.Session() as s:
        s.headers.update(DEFAULT_HEADERS)

        for attempt in range(1, max_retries + 1):
            try:
                r = s.post(API_URL, json=payload, timeout=timeout)
                if 200 <= r.status_code < 300:
                    try:
                        data = r.json() if r.content else {}
                    except Exception:
                        data = {}
                    server_id = data.get("res_id") or data.get("id") or (r.text.strip() if r.text else None)
                    if not server_id:
                        logging.warning("No server id in response: %s", r.text[:300])
                        return None
                    return str(server_id)

                if r.status_code == 409:
                    # duplicate, but treat as sent
                    try:
                        data = r.json() if r.content else {}
                    except Exception:
                        data = {}
                    return str(data.get("res_id") or data.get("id") or "DUPLICATE")

                if r.status_code == 422:
                    logging.error("422 Unprocessable: %s", r.text[:500])
                    return None

                # other errors
                logging.warning("API %s: %s", r.status_code, r.text[:300])
            except requests.RequestException as e:
                logging.warning("API error (try %d): %s", attempt, e)

            if attempt < max_retries:
                time.sleep(attempt)  # 1s, 2s, ...
    return None

def send_unsent_rows(conn: sqlite3.Connection, rate_limit_ms: int = 0) -> int:
    rows = fetch_unsent(conn, BATCH_LIMIT)
    sent_count = 0
    for row_id, user_id, record_time in rows:
        payload = {"user_id": user_id, "time": record_time}
        if API_PASS:
            payload["pass"] = API_PASS

        server_id = post_attendance(payload)
        if server_id:
            mark_sent(conn, row_id, server_id)
            sent_count += 1

        if rate_limit_ms > 0:
            time.sleep(rate_limit_ms / 1000.0)
    return sent_count

# ---------------- Main ----------------
def main():
    conn = connect_db(SQLITE_PATH)

    # 1) pull logs from device and insert into DB
    logs = fetch_logs_with_retries(ZK_IP, ZK_PORT, ZK_TIMEOUT, GET_RETRIES, RECONNECT_DELAY_MS)
    if logs:
        inserted = insert_attendance_bulk(conn, logs)
        logging.info("Inserted new rows: %d", inserted)
    else:
        logging.info("No logs to insert.")

    # 2) send unsent
    sent = send_unsent_rows(conn, rate_limit_ms=0)
    logging.info("Sent to server: %d", sent)

    # 3) optionally clear device logs after successful sends
    if CLEAR_DEVICE_LOGS and sent > 0:
        clear_device_logs()

    conn.close()

if __name__ == "__main__":
    # CLI: python zkteco_sync.py [pull|send|both]
    conn = None
    try:
        action = (sys.argv[1] if len(sys.argv) > 1 else "both").lower()
        if action not in {"pull", "send", "both"}:
            print("Usage: python zkteco_sync.py [pull|send|both]")
            sys.exit(2)

        conn = connect_db(SQLITE_PATH)

        if action in {"pull", "both"}:
            logs = fetch_logs_with_retries(ZK_IP, ZK_PORT, ZK_TIMEOUT, GET_RETRIES, RECONNECT_DELAY_MS)
            if logs:
                ins = insert_attendance_bulk(conn, logs)
                logging.info("Inserted new rows: %d", ins)
            else:
                logging.info("No logs to insert.")

        if action in {"send", "both"}:
            sent = send_unsent_rows(conn, rate_limit_ms=0)
            logging.info("Sent to server: %d", sent)

        if CLEAR_DEVICE_LOGS and action in {"both"}:
            # فقط اگر ارسال داشتیم پاک کن—می‌تونی شرط رو تغییر بدی
            # اینجا برای سادگی بدون شرط پاک نمی‌کنیم
            pass

    finally:
        if conn:
            conn.close()
