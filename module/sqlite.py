import sqlite3
from datetime import *

tag_type = ['[ERROR]', '[INFO]', '[DEBUG]']

def ensure_referral_schema():
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL,
            referred_name TEXT,
            credited INTEGER DEFAULT 0,
            subscribed INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            PRIMARY KEY (referrer_id, referred_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vip_access (
            owner_id INTEGER PRIMARY KEY,
            vip_id INTEGER,
            active INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vip_grants (
            owner_id INTEGER NOT NULL,
            vip_id INTEGER NOT NULL,
            active INTEGER DEFAULT 1,
            updated_at TEXT,
            PRIMARY KEY (owner_id, vip_id)
        )
    """)
    try:
        cur.execute("""
            INSERT OR IGNORE INTO vip_grants (owner_id, vip_id, active, updated_at)
            SELECT owner_id, vip_id, active, updated_at FROM vip_access
            WHERE vip_id IS NOT NULL
        """)
    except sqlite3.OperationalError:
        pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vip_invite_pending (
            invite_link TEXT PRIMARY KEY,
            expected_user_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            vip_id INTEGER,
            created_at TEXT,
            requester_name TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bc_callback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()


def credit_referral(referrer_id: int, referred_id: int, referred_name: str) -> bool:
    """
    Returns True only if referral was credited now (first time).
    """
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row = cur.execute(
        "SELECT credited FROM referrals WHERE referrer_id=? AND referred_id=?",
        (referrer_id, referred_id),
    ).fetchone()

    credited_now = False
    if row is None:
        cur.execute(
            "INSERT INTO referrals(referrer_id, referred_id, referred_name, credited, subscribed, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
            (referrer_id, referred_id, referred_name, 1, 1, now, now),
        )
        credited_now = True
    else:
        if int(row[0]) == 0:
            cur.execute(
                "UPDATE referrals SET credited=1, subscribed=1, referred_name=?, updated_at=? WHERE referrer_id=? AND referred_id=?",
                (referred_name, now, referrer_id, referred_id),
            )
            credited_now = True
        else:
            cur.execute(
                "UPDATE referrals SET referred_name=?, updated_at=? WHERE referrer_id=? AND referred_id=?",
                (referred_name, now, referrer_id, referred_id),
            )

    if credited_now:
        cur.execute("UPDATE users SET ref_count = ref_count + 1 WHERE id = ?", (referrer_id,))

    con.commit()
    con.close()
    return credited_now


def set_referral_subscribed(referrer_id: int, referred_id: int, subscribed: int) -> None:
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "UPDATE referrals SET subscribed=?, updated_at=? WHERE referrer_id=? AND referred_id=?",
        (int(subscribed), now, referrer_id, referred_id),
    )
    con.commit()
    con.close()


def get_referrals(referrer_id: int):
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    rows = cur.execute(
        "SELECT referred_id, referred_name, credited, subscribed FROM referrals WHERE referrer_id=? ORDER BY created_at",
        (referrer_id,),
    ).fetchall()
    con.close()
    return rows


def disqualify_referral(referrer_id: int, referred_id: int) -> bool:
    """
    If referral was credited=1, sets credited=0 (and subscribed=0) and decrements users.ref_count by 1.
    Returns True if it was credited and got disqualified now.
    """
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row = cur.execute(
        "SELECT credited FROM referrals WHERE referrer_id=? AND referred_id=?",
        (referrer_id, referred_id),
    ).fetchone()

    changed = False
    if row is not None and int(row[0]) == 1:
        cur.execute(
            "UPDATE referrals SET credited=0, subscribed=0, updated_at=? WHERE referrer_id=? AND referred_id=?",
            (now, referrer_id, referred_id),
        )
        cur.execute("UPDATE users SET ref_count = CASE WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END WHERE id = ?", (referrer_id,))
        changed = True

    con.commit()
    con.close()
    return changed


def get_referrers_for_reconcile():
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    rows = cur.execute(
        "SELECT DISTINCT referrer_id FROM referrals WHERE credited=1"
    ).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def get_active_vip_holder_ids():
    """Пользователи с хотя бы одним активным VIP (проверка рефералов по расписанию)."""
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    rows = cur.execute(
        "SELECT DISTINCT owner_id FROM vip_grants WHERE active=1"
    ).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def set_vip_grant(owner_id: int, vip_id: int, active: int) -> None:
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "INSERT INTO vip_grants(owner_id, vip_id, active, updated_at) VALUES(?,?,?,?) "
        "ON CONFLICT(owner_id, vip_id) DO UPDATE SET active=excluded.active, updated_at=excluded.updated_at",
        (owner_id, vip_id, int(active), now),
    )
    con.commit()
    con.close()


def has_active_vip_grant(owner_id: int, vip_id: int) -> bool:
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM vip_grants WHERE owner_id=? AND vip_id=? AND active=1",
        (owner_id, vip_id),
    ).fetchone()
    con.close()
    return row is not None


def get_active_vip_grant_vip_ids(owner_id: int) -> list[int]:
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    rows = cur.execute(
        "SELECT vip_id FROM vip_grants WHERE owner_id=? AND active=1 ORDER BY vip_id",
        (owner_id,),
    ).fetchall()
    con.close()
    return [int(r[0]) for r in rows]


def get_vip_grants(owner_id: int):
    """Все привязки VIP-записей к пользователю (для админки): (vip_id, active)."""
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    rows = cur.execute(
        "SELECT vip_id, active FROM vip_grants WHERE owner_id=? ORDER BY vip_id",
        (owner_id,),
    ).fetchall()
    con.close()
    return rows


def deactivate_all_vip_grants(owner_id: int) -> None:
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "UPDATE vip_grants SET active=0, updated_at=? WHERE owner_id=? AND active=1",
        (now, owner_id),
    )
    con.commit()
    con.close()

def add_user(user_id, name, refer_id=None):
    try:
        con = sqlite3.connect('./data/users.sql')
        cur = con.cursor()
        
        cur.execute('SELECT id FROM users WHERE id = ?', (user_id,))
        existing_user = cur.fetchone()
        
        if existing_user:
            if refer_id is not None:
                cur.execute('UPDATE users SET ref_id = ? WHERE id = ?', (refer_id, user_id))
                print(f'{tag_type[1]} Пользователь {name} [ID: {user_id}] обновлен: добавлен ref_id = {refer_id}')
            else:
                print(f'{tag_type[1]} Пользователь {name} [ID: {user_id}] уже существует в базе данных')
        else:
            data = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute('INSERT INTO users(id, name, ref_id, data) VALUES (?, ?, ?, ?)', 
                       (user_id, name, refer_id, data))
            print(f'{tag_type[1]} Пользователь {name} [ID: {user_id}] добавлен в базу данных')
        
        con.commit()
        
    except sqlite3.Error as e:
        print(f"Ошибка базы данных: {e}")
    finally:
        if con:
            con.close()

def check_user(user_id):
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()

    cur.execute(f'SELECT * FROM users WHERE id = {user_id}')
    return cur.fetchone()

def get_user(user_id):
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()

    cur.execute(f'SELECT * FROM users WHERE id = {user_id}')
    return cur.fetchone()

def get_admin():
    return sqlite3.connect('./data/admin.sql').cursor().execute('SELECT id FROM admin').fetchall()

def get_count_users():
    return sqlite3.connect('./data/users.sql').cursor().execute('SELECT COUNT(id) FROM users').fetchone()[0]


def reset_referrer_progress(referrer_id: int) -> None:
    """Обнуляет счётчик прогресса к следующему VIP; строки referrals не удаляем —
    иначе фоновая проверка подписок и отзыв VIP не на кого опираются."""
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    cur.execute("UPDATE users SET ref_count=0 WHERE id=?", (referrer_id,))
    con.commit()
    con.close()


def add_vip_invite_pending(
    invite_link: str,
    expected_user_id: int,
    channel_id: int,
    vip_id: int,
    requester_name: str,
) -> None:
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "INSERT INTO vip_invite_pending(invite_link, expected_user_id, channel_id, vip_id, created_at, requester_name) VALUES(?,?,?,?,?,?)",
        (invite_link, expected_user_id, channel_id, vip_id, now, requester_name),
    )
    con.commit()
    con.close()


def get_vip_invite_pending(invite_link: str):
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    row = cur.execute(
        "SELECT invite_link, expected_user_id, channel_id, vip_id, requester_name FROM vip_invite_pending WHERE invite_link=?",
        (invite_link,),
    ).fetchone()
    con.close()
    return row


def delete_vip_invite_pending(invite_link: str) -> None:
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    cur.execute("DELETE FROM vip_invite_pending WHERE invite_link=?", (invite_link,))
    con.commit()
    con.close()


def list_vip_invite_pending_for_user(expected_user_id: int):
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    rows = cur.execute(
        "SELECT invite_link, channel_id FROM vip_invite_pending WHERE expected_user_id=?",
        (expected_user_id,),
    ).fetchall()
    con.close()
    return rows


def list_vip_invite_pending_for_user_vip(expected_user_id: int, vip_id: int):
    ensure_referral_schema()
    con = sqlite3.connect("./data/users.sql")
    cur = con.cursor()
    rows = cur.execute(
        "SELECT invite_link, channel_id FROM vip_invite_pending WHERE expected_user_id=? AND vip_id=?",
        (expected_user_id, int(vip_id)),
    ).fetchall()
    con.close()
    return rows


def insert_bc_callback_payload(payload: str) -> int:
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    cur.execute("INSERT INTO bc_callback(payload) VALUES(?)", (payload,))
    con.commit()
    rid = cur.lastrowid
    con.close()
    return int(rid)


def get_bc_callback_payload(cb_id: int):
    ensure_referral_schema()
    con = sqlite3.connect('./data/users.sql')
    cur = con.cursor()
    row = cur.execute("SELECT payload FROM bc_callback WHERE id=?", (cb_id,)).fetchone()
    con.close()
    return row[0] if row else None


def get_vip_row(vip_id: int):
    con = sqlite3.connect('./data/vip.sql')
    cur = con.cursor()
    row = cur.execute(
        "SELECT id, name, link, channel_id FROM vip WHERE id=?",
        (vip_id,),
    ).fetchone()
    con.close()
    return row


def get_all_vip_rows():
    con = sqlite3.connect('./data/vip.sql')
    cur = con.cursor()
    rows = cur.execute("SELECT id, name, link, channel_id FROM vip ORDER BY id").fetchall()
    con.close()
    return rows


def update_vip_field(vip_id: int, field: str, value) -> None:
    if field not in ("name", "link", "channel_id"):
        raise ValueError("invalid field")
    con = sqlite3.connect('./data/vip.sql')
    cur = con.cursor()
    cur.execute(f"UPDATE vip SET {field}=? WHERE id=?", (value, vip_id))
    con.commit()
    con.close()


def add_admin_row(user_id: int) -> bool:
    con = sqlite3.connect('./data/admin.sql')
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO admin(id) VALUES(?)", (user_id,))
        con.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        con.close()


def delete_admin_row(user_id: int) -> bool:
    con = sqlite3.connect('./data/admin.sql')
    cur = con.cursor()
    cur.execute("DELETE FROM admin WHERE id=?", (user_id,))
    changed = cur.rowcount > 0
    con.commit()
    con.close()
    return changed