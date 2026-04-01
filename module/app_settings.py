"""Настройки бота в settings/settings.sql (таблица app_kv)."""
import re
import sqlite3

SETTINGS_DB = "./settings/settings.sql"

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$", re.I)

APP_KV_DEFAULTS: dict[str, str] = {
    "ref_count_required": "15",
    "vip_reconcile_interval": "3m",
    "notify_log_chat_id": "",
    "notify_topic_new_user": "",
    "notify_topic_vip_link_issued": "",
    "notify_topic_vip_join_ok": "",
    "notify_topic_vip_intruder": "",
    "notify_topic_vip_access_revoked": "",
    "notify_topic_referral_unsub": "",
    "vip_invite_use_join_request": "1",
}


def ensure_app_kv_schema() -> None:
    con = sqlite3.connect(SETTINGS_DB)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_kv (key TEXT PRIMARY KEY, value TEXT)")
    row = cur.execute("SELECT ref_count FROM settings LIMIT 1").fetchone()
    legacy_ref = str(int(row[0])) if row and row[0] is not None else APP_KV_DEFAULTS["ref_count_required"]
    for k, v in APP_KV_DEFAULTS.items():
        ins = legacy_ref if k == "ref_count_required" else v
        cur.execute("INSERT OR IGNORE INTO app_kv(key,value) VALUES(?,?)", (k, ins))
    con.commit()
    con.close()
    _sync_ref_count_column()


def _sync_ref_count_column() -> None:
    try:
        n = get_ref_count_required()
    except Exception:
        return
    con = sqlite3.connect(SETTINGS_DB)
    cur = con.cursor()
    cur.execute("UPDATE settings SET ref_count=?", (int(n),))
    con.commit()
    con.close()


def get_app_setting(key: str, default: str | None = None) -> str | None:
    ensure_app_kv_schema()
    con = sqlite3.connect(SETTINGS_DB)
    cur = con.cursor()
    row = cur.execute("SELECT value FROM app_kv WHERE key=?", (key,)).fetchone()
    con.close()
    if row is None or row[0] is None or str(row[0]).strip() == "":
        return default
    return str(row[0]).strip()


def get_app_setting_int(key: str) -> int | None:
    v = get_app_setting(key)
    if v is None or v == "":
        return None
    try:
        return int(v)
    except ValueError:
        return None


def set_app_setting(key: str, value: str) -> None:
    ensure_app_kv_schema()
    con = sqlite3.connect(SETTINGS_DB)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO app_kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    con.commit()
    con.close()
    if key == "ref_count_required":
        try:
            con = sqlite3.connect(SETTINGS_DB)
            con.execute("UPDATE settings SET ref_count=?", (int(value),))
            con.commit()
            con.close()
        except Exception:
            pass


def get_ref_count_required() -> int:
    v = get_app_setting("ref_count_required")
    if v is not None:
        try:
            return max(1, int(v))
        except ValueError:
            pass
    con = sqlite3.connect(SETTINGS_DB)
    row = con.cursor().execute("SELECT ref_count FROM settings").fetchone()
    con.close()
    return max(1, int(row[0])) if row and row[0] is not None else 15


def set_ref_count_required(n: int) -> None:
    set_app_setting("ref_count_required", str(max(1, int(n))))


def get_vip_invite_use_join_request() -> bool:
    v = get_app_setting("vip_invite_use_join_request", "1")
    return v not in ("0", "false", "False", "no", "")


def set_vip_invite_use_join_request(on: bool) -> None:
    set_app_setting("vip_invite_use_join_request", "1" if on else "0")


def parse_duration_to_seconds(raw: str) -> int:
    """
    Формат: 30s, 30m, 30h, 3d (регистр не важен, пробелы допускаются).
    Минимум 30 секунд, максимум 30 суток.
    """
    if not raw or not str(raw).strip():
        raise ValueError("пустая строка")
    m = _DURATION_RE.match(str(raw).strip())
    if not m:
        raise ValueError("ожидается формат: 30s, 5m, 2h, 1d")
    n = int(m.group(1))
    u = m.group(2).lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[u]
    sec = n * mult
    if sec < 30:
        raise ValueError("минимум 30s")
    if sec > 30 * 86400:
        raise ValueError("максимум 30d")
    return sec


def get_vip_reconcile_interval_seconds() -> int:
    raw = get_app_setting("vip_reconcile_interval") or "3m"
    try:
        return parse_duration_to_seconds(raw)
    except ValueError:
        return 180


def set_vip_reconcile_interval(raw: str) -> None:
    parse_duration_to_seconds(raw)
    m = _DURATION_RE.match(str(raw).strip())
    if not m:
        raise ValueError("invalid")
    canonical = f"{int(m.group(1))}{m.group(2).lower()}"
    set_app_setting("vip_reconcile_interval", canonical)


def get_all_app_kv() -> dict[str, str]:
    ensure_app_kv_schema()
    con = sqlite3.connect(SETTINGS_DB)
    cur = con.cursor()
    rows = cur.execute("SELECT key, value FROM app_kv ORDER BY key").fetchall()
    con.close()
    return {k: (v or "") for k, v in rows}


def wipe_user_data_tables() -> None:
    """Очищает пользователей, рефералов, VIP-доступ и служебные таблицы в users.sql."""
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    for t in ("referrals", "vip_invite_pending", "bc_callback", "vip_access", "vip_grants", "users"):
        try:
            cur.execute(f"DELETE FROM {t}")
        except sqlite3.OperationalError:
            pass
    con.commit()
    con.close()
