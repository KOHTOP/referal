from telebot import types, TeleBot
import sqlite3
import re

import ctypes
ctypes.windll.kernel32.SetConsoleTitleW("EXAMSFLOW")

import threading
import time as time_module
from datetime import datetime, timedelta

from settings.config import token, SEED_ADMIN_ID
from module.sqlite import *
from module.app_settings import (
    ensure_app_kv_schema,
    get_ref_count_required,
    get_vip_invite_use_join_request,
    set_vip_invite_use_join_request,
    set_app_setting,
    set_ref_count_required,
    get_all_app_kv,
    get_app_setting,
    get_vip_reconcile_interval_seconds,
    set_vip_reconcile_interval,
)
from module.notify import send_topic

bot = TeleBot(token)

ensure_referral_schema()
ensure_app_kv_schema()
if SEED_ADMIN_ID and not get_admin():
    add_admin_row(int(SEED_ADMIN_ID))

waiting_for_ad = {}
waiting_for_ad_kb = {}
waiting_for_cfg_key = {}
waiting_for_user_lookup = {}
waiting_for_gift_target = {}
waiting_for_admin_add = {}
waiting_for_vip_edit = {}
last_message = {}

CFG_MENU = [
    ("ref_count_required", "👥 Рефералов для VIP"),
    ("vip_reconcile_interval", "⏱ Проверка VIP (интервал)"),
    ("notify_log_chat_id", "💬 ID чата логов"),
    ("notify_topic_new_user", "📌 Тема: новые пользователи"),
    ("notify_topic_vip_link_issued", "📌 Тема: выдача VIP-ссылки"),
    ("notify_topic_vip_join_ok", "📌 Тема: вход в VIP (успех)"),
    ("notify_topic_vip_intruder", "📌 Тема: чужой вход"),
    ("notify_topic_vip_access_revoked", "📌 Тема: отзыв VIP"),
    ("notify_topic_referral_unsub", "📌 Тема: рефералы отписались"),
]

CFG_HINTS = {
    "ref_count_required": "✍️ Введите <b>целое число</b> — сколько приглашённых нужно для VIP (минимум 1).",
    "vip_reconcile_interval": (
        "✍️ Интервал <b>фоновой проверки</b> держателей VIP: рефералы и подписки на каналы.\n\n"
        "Формат: число + буква — <code>s</code> сек, <code>m</code> мин, <code>h</code> ч, <code>d</code> дни.\n"
        "Примеры: <code>30s</code> <code>5m</code> <code>1h</code> <code>3d</code>\n"
        "Минимум <code>30s</code>, максимум <code>30d</code>."
    ),
    "notify_log_chat_id": "✍️ Введите <b>ID супергруппы</b> для логов (число). Пусто или <code>-</code> — отключить логи.",
    "notify_topic_new_user": "✍️ Введите <b>ID темы</b> (message_thread_id) или <code>-</code> чтобы писать в общий чат.",
    "notify_topic_vip_link_issued": "✍️ ID темы для «выдана VIP-ссылка» или <code>-</code>.",
    "notify_topic_vip_join_ok": "✍️ ID темы для «успешный вход в VIP» или <code>-</code>.",
    "notify_topic_vip_intruder": "✍️ ID темы для «чужой вход» или <code>-</code>.",
    "notify_topic_vip_access_revoked": "✍️ ID темы для «отозван VIP» или <code>-</code>.",
    "notify_topic_referral_unsub": "✍️ ID темы для «рефералы отписались» или <code>-</code>.",
}
ad_count = 0
fail_count = 0

BROADCAST_KB_MARKER = "\n---KEYBOARD---\n"
_BTN_URL = re.compile(r"^(.+?)\s+-\s+url:\s*(.+)$", re.DOTALL)
_BTN_CALL = re.compile(r"^(.+?)\s+-\s+call:\s*(.+)$", re.DOTALL)

_last_block_check = {}


def _fmt_timestamp() -> str:
    return datetime.now().strftime("%d.%m.%Y · %H:%M:%S")


def _fmt_user_block(user_id: int, display_name: str | None = None) -> str:
    username = None
    try:
        ch = bot.get_chat(user_id)
        if getattr(ch, "username", None):
            username = ch.username
        if not display_name and getattr(ch, "full_name", None):
            display_name = ch.full_name
    except Exception:
        pass
    name = display_name or "—"
    line2 = f"🆔 <code>{user_id}</code>"
    if username:
        line2 += f" · @{username}"
    return (
        f"👤 <b>{name}</b>\n"
        f"{line2}\n"
        f'🔗 <a href="tg://user?id={user_id}">открыть профиль</a>'
    )


def _log_banner(emoji: str, title: str) -> str:
    return f"{emoji} <b>{title}</b>\n<i>🕐 {_fmt_timestamp()}</i>\n"


def _vip_notify_lines(vip_id: int, vrow) -> str:
    if not vrow:
        return f"📁 VIP · id <code>{vip_id}</code>"
    _i, vname, vlink, vch = vrow
    lines = [f"📁 <b>{vname}</b> · запись <code>{vip_id}</code>"]
    if vch is not None:
        lines.append(f"📢 channel_id: <code>{vch}</code>")
    if vlink:
        lines.append(f'🔗 <a href="{vlink}">статическая ссылка в БД</a>')
    return "\n".join(lines)


def kick_user_from_channel(channel_id: int, user_id: int) -> None:
    try:
        bot.ban_chat_member(channel_id, user_id)
        bot.unban_chat_member(channel_id, user_id)
    except Exception as e:
        print(f"kick_user_from_channel: {e}")


def revoke_all_pending_for_user(user_id: int) -> None:
    for invite_link, channel_id in list_vip_invite_pending_for_user(user_id):
        try:
            bot.revoke_chat_invite_link(channel_id, invite_link)
        except Exception:
            pass
        delete_vip_invite_pending(invite_link)


def revoke_pending_for_user_vip(user_id: int, vip_id: int) -> None:
    """Отзыв только инвайтов по конкретной VIP-записи — чтобы другие каналы не теряли ссылку."""
    for invite_link, channel_id in list_vip_invite_pending_for_user_vip(user_id, vip_id):
        try:
            bot.revoke_chat_invite_link(channel_id, invite_link)
        except Exception:
            pass
        delete_vip_invite_pending(invite_link)


def issue_vip_access(user_id: int, full_name: str, vip_id: int, *, reset_refs: bool, notify_link: bool = True) -> tuple[bool, str | None]:
    """Одноразовая ссылка (member_limit=1). Возвращает (ok, сообщение_ошибки_или_None)."""
    revoke_pending_for_user_vip(user_id, vip_id)
    row = get_vip_row(vip_id)
    if not row:
        return False, "VIP не найден."

    _vid, vname, static_link, channel_id = row

    if not channel_id:
        set_vip_grant(user_id, vip_id, 1)
        if reset_refs:
            reset_referrer_progress(user_id)
        if static_link:
            bot.send_message(
                user_id,
                f"🎉 <b>Поздравляем с VIP!</b>\n\n"
                f"<blockquote>🔗 <b>Ваша ссылка</b>\n{static_link}</blockquote>\n\n"
                f"💡 <i>Администратору: добавьте <code>channel_id</code> в карточке VIP — "
                f"тогда бот сможет выдавать персональные инвайты.</i>",
                parse_mode="HTML",
            )
        if notify_link:
            send_topic(
                bot,
                "NOTIFY_TOPIC_VIP_LINK_ISSUED",
                f"{_log_banner('⚠️', 'VIP: статическая ссылка (нет channel_id)')}"
                f"{_fmt_user_block(user_id, full_name)}\n"
                f"{_vip_notify_lines(vip_id, row)}\n"
                f"<blockquote>Инвайт через API недоступен — пользователю отправлена ссылка из БД.</blockquote>",
            )
        return True, None

    use_jr = get_vip_invite_use_join_request()
    try:
        # Telegram: нельзя одновременно creates_join_request и member_limit.
        if use_jr:
            inv = bot.create_chat_invite_link(
                int(channel_id),
                name=f"u{user_id}"[:32],
                creates_join_request=True,
            )
        else:
            inv = bot.create_chat_invite_link(
                int(channel_id),
                name=f"u{user_id}"[:32],
                member_limit=1,
                creates_join_request=False,
            )
        url = inv.invite_link
    except Exception as e:
        print(f"create_chat_invite_link: {e}")
        if static_link:
            set_vip_grant(user_id, vip_id, 1)
            if reset_refs:
                reset_referrer_progress(user_id)
            bot.send_message(
                user_id,
                f"🎉 <b>VIP готов</b> (резервный режим)\n\n"
                f"<blockquote>🔗 {static_link}</blockquote>\n\n"
                f"⚠️ <i>Не удалось создать персональный инвайт — дана ссылка из базы. "
                f"Если не открывается, напишите в поддержку.</i>",
                parse_mode="HTML",
            )
            if notify_link:
                send_topic(
                    bot,
                    "NOTIFY_TOPIC_VIP_LINK_ISSUED",
                    f"{_log_banner('❌', 'Ошибка создания VIP-инвайта')}"
                    f"{_fmt_user_block(user_id, full_name)}\n"
                    f"{_vip_notify_lines(vip_id, row)}\n"
                    f"<blockquote><b>Ошибка API:</b>\n<code>{e}</code>\n\nВыдана резервная ссылка из БД.</blockquote>",
                )
            return True, None
        return False, str(e)

    add_vip_invite_pending(url, user_id, int(channel_id), vip_id, full_name or str(user_id))
    set_vip_grant(user_id, vip_id, 1)
    if reset_refs:
        reset_referrer_progress(user_id)

    if use_jr:
        link_hint = (
            "<b>Ваша ссылка (заявка в канал):</b>\n🔗 {url}\n\n"
            "Перейдите по ссылке и отправьте заявку — бот одобрит только вас.\n"
            "⚠️ Не передавайте ссылку другим."
        ).format(url=url)
    else:
        link_hint = (
            "<b>Персональная ссылка (один вход):</b>\n🔗 {url}\n\n"
            "⚠️ Не передавайте её другим."
        ).format(url=url)

    if reset_refs:
        user_txt = (
            f"╔══════════════════════╗\n"
            f"║  🎉 <b>VIP — ВЫ ДОСТИГЛИ!</b>  ║\n"
            f"╚══════════════════════╝\n\n"
            f"{link_hint}\n\n"
            f"✨ <i>Счётчик приглашений сброшен — можно снова копить друзей на новый канал.</i>"
        )
    else:
        user_txt = (
            f"✦ <b>Доступ обновлён</b> ✦\n\n"
            f"{link_hint}"
        )
    bot.send_message(user_id, user_txt, parse_mode="HTML")

    if notify_link:
        mode = "заявка → автопринятие" if use_jr else "прямой вход (1 вход)"
        send_topic(
            bot,
            "NOTIFY_TOPIC_VIP_LINK_ISSUED",
            f"{_log_banner('🔗', 'Выдана персональная VIP-ссылка')}"
            f"{_fmt_user_block(user_id, full_name)}\n"
            f"{_vip_notify_lines(vip_id, row)}\n"
            f"<b>Режим:</b> {mode}\n"
            f"<blockquote>Инвайт:\n<code>{url[:80]}{'…' if len(url) > 80 else ''}</code></blockquote>",
        )
    return True, None


def split_broadcast_text(raw: str | None):
    if not raw:
        return "", None
    if BROADCAST_KB_MARKER not in raw:
        return raw, None
    main, kb = raw.split(BROADCAST_KB_MARKER, 1)
    return main.rstrip(), kb.strip() or None


def parse_keyboard_spec(kb_str: str):
    rows_out = []
    for line in kb_str.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        row = []
        for part in line.split("|"):
            part = part.strip()
            if not part:
                continue
            m = _BTN_URL.match(part)
            if m:
                row.append(("url", m.group(1).strip(), m.group(2).strip()))
                continue
            m = _BTN_CALL.match(part)
            if m:
                row.append(("call", m.group(1).strip(), m.group(2).strip()))
                continue
        if row:
            rows_out.append(row)
    return rows_out


def build_broadcast_markup(rows):
    if not rows:
        return None
    kb = types.InlineKeyboardMarkup()
    for r in rows:
        btns = []
        for kind, label, val in r:
            label = (label or "?")[:64]
            if kind == "url":
                btns.append(types.InlineKeyboardButton(text=label, url=val.strip()))
            else:
                cb_id = insert_bc_callback_payload(val[:500])
                data = f"bcid:{cb_id}"
                if len(data.encode("utf-8")) > 64:
                    data = data[:64]
                btns.append(types.InlineKeyboardButton(text=label, callback_data=data))
        if btns:
            kb.row(*btns)
    return kb


def build_cfg_hub_content():
    d = get_all_app_kv()
    jr = get_vip_invite_use_join_request()
    topics_preview = []
    for k, lbl in CFG_MENU[3:]:
        v = d.get(k) or "—"
        short = lbl.replace("📌 Тема: ", "")
        topics_preview.append(f"  ▸ {short}: <code>{v}</code>")
    tp = "\n".join(topics_preview[:4])
    if len(topics_preview) > 4:
        tp += f"\n  <i>… ещё {len(topics_preview) - 4} тем — раздел «Логи»</i>"
    iv_raw = d.get("vip_reconcile_interval") or "3m"
    iv_sec = get_vip_reconcile_interval_seconds()
    text = (
        "╔══════════════════════════╗\n"
        "║  🤖 <b>ПАРАМЕТРЫ БОТА</b>  ║\n"
        "╚══════════════════════════╝\n\n"
        f"👥 <b>Рефералов для VIP</b> → <code>{get_ref_count_required()}</code>\n"
        f"⏱ <b>Проверка VIP</b> → <code>{iv_raw}</code> (~<code>{iv_sec}</code> сек)\n"
        f"<blockquote>По таймеру бот проверяет всех с <b>активным VIP</b>: подписки рефералов; "
        f"при нарушении — отзыв доступа и уведомления.</blockquote>\n"
        f"🔐 <b>Режим инвайта</b> → {'<i>заявка + авто</i>' if jr else '<i>1 вход по ссылке</i>'}\n"
        f"💬 <b>Чат логов</b> → <code>{d.get('notify_log_chat_id') or '—'}</code>\n\n"
        f"📣 <b>Темы</b> (кратко)\n{tp}\n\n"
        f"<i>Логи и все темы — экран «Логи и темы».</i>"
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(text="✏️ Рефералы", callback_data="admin:cfg:e:0"),
        types.InlineKeyboardButton(text="✏️ Интервал VIP", callback_data="admin:cfg:e:1"),
    )
    kb.add(types.InlineKeyboardButton(text="📣 Логи и темы", callback_data="admin:cfg:sub:notify"))
    kb.add(types.InlineKeyboardButton(text="🔁 Переключить VIP-инвайт", callback_data="admin:cfg:tvip"))
    kb.add(types.InlineKeyboardButton(text="◀️ В настройки", callback_data="admin:settings"))
    return text, kb


def build_cfg_notify_content():
    d = get_all_app_kv()
    lines = []
    for k, lbl in CFG_MENU[2:]:
        v = d.get(k) or "—"
        short = lbl.replace("📌 Тема: ", "").replace("💬 ", "")
        lines.append(f"  ▸ <b>{short}</b>\n     <code>{v}</code>")
    text = (
        "📣 <b>Логи и темы форума</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(lines)
        + "\n━━━━━━━━━━━━━━━━━━━━\n"
        "<i>✏️ — изменить. Сброс темы: отправьте <code>-</code></i>\n\n"
        "<i>Интервал проверки VIP — в «Параметры бота» → ✏️ Интервал VIP.</i>"
    )
    kb = types.InlineKeyboardMarkup()
    for i in range(2, len(CFG_MENU)):
        lab = CFG_MENU[i][1]
        kb.add(
            types.InlineKeyboardButton(
                text=f"✏️ {lab}"[:64],
                callback_data=f"admin:cfg:e:{i}",
            )
        )
    kb.add(types.InlineKeyboardButton(text="◀️ К параметрам", callback_data="admin:cfg:hub"))
    return text, kb


def _build_vip_help_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    vip_rows = sqlite3.connect("./data/vip.sql").cursor().execute(
        "SELECT id, name FROM vip ORDER BY id"
    ).fetchall()
    owned = set(get_active_vip_grant_vip_ids(user_id))
    vip_buttons = []
    for vid, name in vip_rows:
        vid = int(vid)
        if vid in owned:
            vip_buttons.append(
                types.InlineKeyboardButton(text=f"🔗 {name}", callback_data=f"vipresend:{vid}")
            )
        else:
            vip_buttons.append(types.InlineKeyboardButton(text=name, callback_data=f"get:{vid}"))
    kb = types.InlineKeyboardMarkup()
    for i in range(0, len(vip_buttons), 2):
        kb.row(*vip_buttons[i : i + 2])
    return kb


def fmt_main_menu_text(full_name: str, user_id: int, ref_count: int, ref_max: int, bot_username: str) -> str:
    pct = min(100, int(ref_count * 100 / ref_max)) if ref_max else 0
    bar_filled = max(0, min(10, pct // 10))
    bar = "█" * bar_filled + "░" * (10 - bar_filled)
    return (
        f"Привет, <b>{full_name}</b> 👋\n\n"
        f"╭─────────────────────────╮\n"
        f"│  🎁 <b>Бесплатный VIP</b> за друзей   │\n"
        f"╰─────────────────────────╯\n\n"
        f"Пригласи <code>{ref_max}</code> человек по своей ссылке — "
        f"и откроется <b>один</b> выбранный <i>VIP-канал</i>. "
        f"За следующий цикл приглашений можно открыть ещё канал — каждый идёт отдельно.\n\n"
        f"📈 <b>Прогресс</b>\n"
        f"<code>[{bar}]</code> <b>{pct}%</b>\n"
        f"👥 Друзей: <code>{ref_count}</code> / <code>{ref_max}</code>\n\n"
        f"✨ <b>Каналы VIP</b>\n"
        f"<blockquote><i>· 🗣 Устное собеседование\n· 📚 ОГЭ \n· 📝 ВПР \n· ✍️ Итоговое сочинение \n· 🏆 Олимпиады \n· 📊 Пробники</i></blockquote>\n\n"
        f"🔗 <b>Твоя ссылка</b>\n"
        f"<tg-spoiler>https://t.me/{bot_username}?start=ref_{user_id}</tg-spoiler>\n\n"
        f"⚡ Когда наберёшь <code>{ref_max}</code> — в разделе «Помощь» можно открыть <b>ещё один</b> VIP-канал."
    )


def send_mailing_action_keyboard(chat_id: int, admin_uid: int) -> None:
    if admin_uid not in last_message:
        return
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(text="🔘 Добавить кнопки", callback_data="admin:ad_kb"),
        types.InlineKeyboardButton(text="✅ Отправить всем", callback_data="admin:ad_send"),
    )
    kb.add(types.InlineKeyboardButton(text="◀️ В меню", callback_data="admin:menu"))
    bot.send_message(
        chat_id,
        "╭──────────────────╮\n"
        "│ <b>Следующий шаг</b> │\n"
        "╰──────────────────╯\n\n"
        "🔘 <b>Добавить кнопки</b> — описать клавиатуру сообщением\n"
        "✅ <b>Отправить всем</b> — разослать базе\n"
        "◀️ <b>В меню</b> — выход без отправки",
        parse_mode="HTML",
        reply_markup=kb,
    )


def is_bot_blocked(user_id: int) -> bool:
    """
    Best-effort detection. Telegram doesn't provide a clean 'blocked' check;
    sending a chat_action is lightweight and throws 403 if blocked.
    """
    try:
        bot.send_chat_action(user_id, 'typing')
        return False
    except Exception as e:
        s = str(e).lower()
        return ('blocked' in s) or ('forbidden' in s) or ('403' in s)


def reconcile_referrer_state(referrer_id: int) -> tuple[int, int, bool, bool]:
    """
    Returns: (credited_count, required_count, all_subscribed, has_unsubscribed)
    Also sends notifications to referrer on subscription state changes.
    """
    required = get_ref_count_required()
    referrals = get_referrals(referrer_id)
    credited = [r for r in referrals if int(r[2]) == 1]

    all_subscribed = True
    has_unsubscribed = False

    active_grant_ids = get_active_vip_grant_vip_ids(referrer_id)
    vip_active = len(active_grant_ids) > 0

    for referred_id, referred_name, credited_flag, subscribed_flag in credited:
        is_sub = bool(check_subscription(int(referred_id)))

        # if referred blocked bot, disqualify (deduct) the referral
        # to avoid spam, run this check at most once per 6 hours per referred_id
        if vip_active:
            now_dt = datetime.now()
            last_dt = _last_block_check.get(int(referred_id))
            if last_dt is None or (now_dt - last_dt) >= timedelta(hours=6):
                _last_block_check[int(referred_id)] = now_dt
                if is_bot_blocked(int(referred_id)):
                    if disqualify_referral(referrer_id, int(referred_id)):
                        try:
                            bot.send_message(
                                referrer_id,
                                f"📵 <b>Реферал обновлён</b>\n\n"
                                f"Пользователь <b>{referred_name}</b> заблокировал бота — "
                                f"засчитанный реферал снят со счёта.",
                                parse_mode="HTML",
                            )
                        except Exception:
                            pass
                    is_sub = False

        if is_sub != bool(int(subscribed_flag)):
            set_referral_subscribed(referrer_id, int(referred_id), 1 if is_sub else 0)

            if not is_sub:
                has_unsubscribed = True
                try:
                    bot.send_message(
                        referrer_id,
                        f"📉 <b>Реферал отписался</b>\n\n"
                        f"<b>{referred_name}</b> больше не подписан на обязательные каналы.\n"
                        f"<i>Пока не все подпишутся снова — VIP может быть приостановлен.</i>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            # if re-subscribed we just update silently

        if not is_sub:
            all_subscribed = False

    if not all_subscribed and vip_active:
        titles = []
        notify_blocks = []
        for vid in active_grant_ids:
            vr = get_vip_row(vid)
            if vr and vr[3]:
                kick_user_from_channel(int(vr[3]), referrer_id)
            if vr:
                titles.append(vr[1])
            notify_blocks.append(_vip_notify_lines(int(vid), vr))
        revoke_all_pending_for_user(referrer_id)
        deactivate_all_vip_grants(referrer_id)
        vtitle = ", ".join(titles) if titles else "VIP"
        vip_notify_combined = "\n".join(notify_blocks) if notify_blocks else ""
        ref_row = get_user(referrer_id)
        ref_display = ref_row[1] if ref_row else None
        bad_refs = []
        for referred_id, referred_name, cf, _sf in credited:
            if int(cf) == 1 and not check_subscription(int(referred_id)):
                bad_refs.append((int(referred_id), referred_name))
        bad_lines = "\n".join(f"  • <b>{nm}</b> · <code>{rid}</code>" for rid, nm in bad_refs[:12])
        if len(bad_refs) > 12:
            bad_lines += f"\n  <i>… и ещё {len(bad_refs) - 12}</i>"
        if not bad_lines:
            bad_lines = "  <i>(детали — не все засчитанные подписаны)</i>"
        try:
            bot.send_message(
                referrer_id,
                f"🛡 <b>VIP приостановлен</b>\n\n"
                f"Не все приглашённые подписаны на каналы — доступ ко <b>всем</b> вашим VIP-каналам снят, "
                f"вы исключены из них до восстановления подписок.\n\n"
                f"<blockquote>Нужно рефералов: <code>{required}</code>\n"
                f"Засчитано в базе: <code>{len(credited)}</code></blockquote>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        detail_revoke = (
            f"{_log_banner('🚫', 'VIP отозван · исключение из канала')}"
            f"{_fmt_user_block(referrer_id, ref_display)}\n"
            f"{vip_notify_combined}\n"
            f"<b>Причина:</b> не все рефералы на обязательных каналах\n"
            f"<blockquote><b>Сейчас не ОК по подписке:</b>\n{bad_lines}</blockquote>"
        )
        send_topic(bot, "NOTIFY_TOPIC_VIP_ACCESS_REVOKED", detail_revoke)
        send_topic(
            bot,
            "NOTIFY_TOPIC_REFERRAL_UNSUB",
            f"{_log_banner('📉', 'Реферальная цепочка нарушена')}"
            f"{_fmt_user_block(referrer_id, ref_display)}\n"
            f"📊 Засчитанных рефералов: <code>{len(credited)}</code> · нужно для VIP: <code>{required}</code>\n"
            f"🎟 VIP: <b>{vtitle}</b>\n"
            f"<blockquote>Действие: все активные VIP деактивированы, инвайты отозваны.</blockquote>",
        )

    return (len(credited), required, all_subscribed, has_unsubscribed)


def background_reconcile_loop():
    while True:
        sleep_sec = max(30, get_vip_reconcile_interval_seconds())
        try:
            for owner_id in get_active_vip_holder_ids():
                reconcile_referrer_state(int(owner_id))
        except Exception:
            pass
        time_module.sleep(sleep_sec)

@bot.message_handler(commands=['start'])
def command_start(message, ref_id=None, callback=False):
    if not callback:
        arg = message.text.split(' ')
        ref_id = arg[1].split('_')[1] if len(arg) > 1 else None

    if not check_user(message.from_user.id):
        if ref_id and check_subscription(message.from_user.id):
            add_user(message.from_user.id, message.from_user.full_name, ref_id)
            credited_now = credit_referral(int(ref_id), message.from_user.id, message.from_user.full_name)
            if credited_now:
                bot.send_message(
                    ref_id,
                    f"✨ <b>Новый реферал!</b>\n\n"
                    f"По вашей ссылке зашёл <b>{message.from_user.full_name}</b>.\n"
                    f"<i>Так держать — цель ближе!</i>",
                    parse_mode="HTML",
                )
        else:
            add_user(message.from_user.id, message.from_user.full_name)
        ref_line = ""
        if ref_id:
            try:
                rrow = get_user(int(ref_id))
                rnm = rrow[1] if rrow else None
                ref_line = f"\n🎯 <b>Пригласил:</b>\n{_fmt_user_block(int(ref_id), rnm)}"
            except Exception:
                ref_line = f"\n🎯 Реферер ID: <code>{ref_id}</code>"
        un = getattr(message.from_user, "username", None)
        un_line = f"@{un}" if un else "<i>без username</i>"
        send_topic(
            bot,
            "NOTIFY_TOPIC_NEW_USER",
            f"{_log_banner('✨', 'Новая регистрация в боте')}"
            f"{_fmt_user_block(message.from_user.id, message.from_user.full_name)}\n"
            f"📛 {un_line}\n"
            f"{ref_line}\n"
            f"<blockquote>Язык: {getattr(message.from_user, 'language_code', '—') or '—'}</blockquote>",
        )

    if check_subscription(message.from_user.id):
        ref_count_max = get_ref_count_required()
        ref_count = get_user(message.from_user.id)[2]

        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton(text="👥 Пригласить друзей", url=f'https://t.me/share/url?url=https://t.me/{bot.get_me().username}?start=ref_{message.from_user.id}&text=Привет! Заходи в этого крутого бота за бесплатными ответами!'))
        kb.add(types.InlineKeyboardButton(text="📊 Мой прогресс", callback_data=f'progress'),
        types.InlineKeyboardButton(text="❓ Помощь", callback_data=f'help'))
        bot.send_message(
            message.chat.id,
            fmt_main_menu_text(
                message.from_user.full_name,
                message.from_user.id,
                ref_count,
                ref_count_max,
                bot.get_me().username,
            ),
            reply_markup=kb,
            parse_mode="HTML",
        )
    else:
        keyboard_sub(1, message.chat.id, ref_id=ref_id)

@bot.message_handler(commands=['admin', 'panel'])
def command_admin(message):
    if message.from_user.id in [admin[0] for admin in get_admin()]:
        kb = types.InlineKeyboardMarkup()
        kb.add(
            types.InlineKeyboardButton(text="📈 Статистика", callback_data="admin:stats"),
            types.InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users"),
        )
        kb.add(
            types.InlineKeyboardButton(text="✉️ Рассылка", callback_data="admin:ad"),
            types.InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin:settings"),
        )

        bot.send_message(
            message.chat.id,
            "╔══════════════════════════╗\n"
            "║   🛡 <b>ADMIN CONSOLE</b>   ║\n"
            "╚══════════════════════════╝\n\n"
            "Выбери раздел — всё в одном месте:\n"
            "📈 цифры · 👥 люди · ✉️ рассылка · ⚙️ настройки",
            parse_mode="HTML",
            reply_markup=kb,
        )

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    if call.data.startswith("bcid:"):
        try:
            cid = int(call.data.split(":", 1)[1])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка кнопки", True)
            return
        payload = get_bc_callback_payload(cid)
        if not payload:
            bot.answer_callback_query(call.id, "Кнопка устарела", True)
            return
        txt = payload[:180] if len(payload) > 180 else payload
        bot.answer_callback_query(call.id, txt, show_alert=len(txt) > 0)
        return

    if call.data.startswith('check_sub'):
        ref_id = call.data.split('_')[2] if len(call.data.split('_')) > 2 else None
        if check_subscription(call.from_user.id):
            if ref_id:
                add_user(call.from_user.id, call.from_user.full_name, ref_id)
                credited_now = credit_referral(int(ref_id), call.from_user.id, call.from_user.full_name)
                if credited_now:
                    bot.send_message(
                        ref_id,
                        f"✨ <b>Новый реферал!</b>\n\n"
                        f"По вашей ссылке зашёл <b>{call.from_user.full_name}</b>.",
                        parse_mode="HTML",
                    )

            ref_count_max = get_ref_count_required()
            ref_count = get_user(call.from_user.id)[2]
            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton(text="👥 Пригласить друзей", url=f'https://t.me/share/url?url=https://t.me/{bot.get_me().username}?start=ref_{call.from_user.id}&text=Привет! Заходи в этого крутого бота за бесплатными ответами!'))
            kb.add(types.InlineKeyboardButton(text="📊 Мой прогресс", callback_data=f'progress'),
            types.InlineKeyboardButton(text="❓ Помощь", callback_data=f'help'))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=fmt_main_menu_text(
                    call.from_user.full_name,
                    call.from_user.id,
                    ref_count,
                    ref_count_max,
                    bot.get_me().username,
                ),
                reply_markup=kb,
                parse_mode="HTML",
            )
        else:
            try:
                keyboard_sub(2, call.from_user.id, ref_id=ref_id)
            except:
                bot.answer_callback_query(call.id, 'Вы не подписаны на каналы!', show_alert=True)
    elif call.data == 'progress':
        ref_count_max = get_ref_count_required()
        ref_count = get_user(call.from_user.id)[2]
        ref_users = sqlite3.connect('./data/users.sql').cursor().execute(f'SELECT id FROM users WHERE ref_id = {call.from_user.id} LIMIT 5').fetchall()

        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton(text="👥 Пригласить друзей", url=f'https://t.me/share/url?url=https://t.me/{bot.get_me().username}?start=ref_{call.from_user.id}&text=Привет! Заходи в этого крутого бота за бесплатными ответами!'))
        kb.add(types.InlineKeyboardButton(text="📊 Мой прогресс", callback_data=f'progress'),
        types.InlineKeyboardButton(text="❓ Помощь", callback_data=f'help'))
        credited_count, required, all_subscribed, _ = reconcile_referrer_state(call.from_user.id)
        if credited_count >= required and not all_subscribed:
            kb.add(types.InlineKeyboardButton(text="🔄 Запросить доступ снова", callback_data="recheck_access"))
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=fmt_main_menu_text(
                    call.from_user.full_name,
                    call.from_user.id,
                    ref_count,
                    ref_count_max,
                    bot.get_me().username,
                ),
                reply_markup=kb,
                parse_mode="HTML",
            )
            bot.answer_callback_query(call.id, f"📊 Рефералов: {ref_count} / {ref_count_max}", True)
        except Exception:
            bot.answer_callback_query(call.id, f"📊 Рефералов: {ref_count} / {ref_count_max}", True)
    elif call.data == 'help':
        kb = _build_vip_help_keyboard(call.from_user.id)

        ref_count_max = get_ref_count_required()

        kb.add(types.InlineKeyboardButton(text="< Назад", callback_data="progress"))
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
        text=f"""✦ <b>VIP — ЧТО ВНУТРИ</b> ✦

Кнопка с названием — <b>открыть канал</b> за новый цикл приглашений (счётчик друзей в главном меню).
Кнопка <b>🔗</b> у канала — <b>повторить ссылку</b> уже открытого VIP (рефералы должны быть подписаны).

<i>Обзор направлений:</i>

<b>🗣 Устное собеседование</b>
<blockquote><i>• Полные варианты заданий
• Точные ответы на УС
• Критерии оценки экспертов
• Советы по подготовке</i></blockquote>

<b>📚 ОГЭ 2026</b>
<blockquote><i>• Варианты по всем предметам
• Ответы и решения
• Предметные шпаргалки
• Разбор сложных заданий</i></blockquote>

<b>📝 ВПР 2026</b>
<blockquote><i>• Варианты ВПР для всех классов
• Ответы с пояснениями
• Критерии оценивания
• Тренировочные работы</i></blockquote>

<b>✍️ Итоговое сочинение</b>
<blockquote><i>• Банк аргументов
• Примеры готовых сочинений
• Списки литературы
• Темы по всем направлениям
• Структура идеального сочинения</i></blockquote>

<b>🏆 Олимпиады</b>
<blockquote><i>• Решения и ответы
• Методички по предметам</i></blockquote>

<b>📊 Пробники</b>
<blockquote><i>• Точные ответы
• Полные варианты и решения
• Пробники для каждого региона</i></blockquote>

💡 <b>Как открыть ещё один VIP?</b>
<blockquote>Набери <code>{ref_count_max}</code> друзей в текущем цикле (прогресс в главном меню), все — с подпиской на обязательные каналы, затем нажми название нужного VIP.</blockquote>

<b>Есть вопросы?</b> <i>Пиши @managerexams</i>""", parse_mode="HTML", reply_markup=kb)

    elif call.data.startswith("vipresend:"):
        try:
            vip_id = int(call.data.split(":")[1])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка кнопки", True)
            return
        uid = call.from_user.id
        _, _req, all_subscribed, _ = reconcile_referrer_state(uid)
        if not has_active_vip_grant(uid, vip_id):
            bot.answer_callback_query(
                call.id,
                "Нет активного доступа к этому VIP — откройте его через кнопку с названием после нового цикла приглашений.",
                True,
            )
            return
        if not all_subscribed:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="🔄 Запросить доступ снова", callback_data="recheck_access"))
            bot.send_message(
                uid,
                "⏳ <b>Сначала подписки рефералов</b>\n\n"
                "Пока не все приглашённые на обязательных каналах — ссылку обновить нельзя.",
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id, "Восстановите подписки рефералов", True)
            return
        ok, err = issue_vip_access(
            uid,
            call.from_user.full_name or str(uid),
            vip_id,
            reset_refs=False,
            notify_link=True,
        )
        if ok:
            bot.answer_callback_query(call.id, "Новая ссылка отправлена в чат.", False)
        else:
            bot.answer_callback_query(call.id, err or "Ошибка", True)

    elif call.data.startswith("get:"):
        try:
            vip_id = int(call.data.split(":")[1])
        except Exception:
            bot.answer_callback_query(call.id, "Ошибка кнопки", True)
            return
        uid = call.from_user.id
        _, required, all_subscribed, _ = reconcile_referrer_state(uid)
        ref_count = get_user(uid)[2]
        if has_active_vip_grant(uid, vip_id):
            bot.answer_callback_query(
                call.id,
                "Этот VIP уже открыт — в «Помощь» нажмите 🔗 у названия для новой ссылки.",
                True,
            )
            return
        if ref_count < required or not all_subscribed:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="🔄 Запросить доступ снова", callback_data="recheck_access"))
            bot.send_message(
                uid,
                "⏳ <b>VIP пока недоступен</b>\n\n"
                "Нужен <b>текущий</b> прогресс приглашений в главном меню "
                f"(<code>{ref_count}</code> / <code>{required}</code>) "
                "и подписка всех засчитанных рефералов на обязательные каналы.\n\n"
                "После нового цикла друзей снова откройте «Помощь» и выберите канал.",
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id, "Нужен новый цикл приглашений или подписки", True)
            return
        ok, err = issue_vip_access(
            uid,
            call.from_user.full_name or str(uid),
            vip_id,
            reset_refs=True,
            notify_link=True,
        )
        if ok:
            bot.answer_callback_query(call.id, "Готово! Ссылка отправлена в чат.", False)
        else:
            bot.answer_callback_query(call.id, err or "Ошибка", True)

    elif call.data == 'recheck_access':
        uid = call.from_user.id
        _, required, all_subscribed, _ = reconcile_referrer_state(uid)
        ref_count = get_user(uid)[2]
        owned = get_active_vip_grant_vip_ids(uid)
        if not all_subscribed:
            bot.answer_callback_query(call.id, 'Пока что не все рефералы подписаны на каналы.', True)
            return
        if len(owned) == 0:
            if ref_count >= required:
                kb = _build_vip_help_keyboard(uid)
                bot.send_message(
                    uid,
                    "✅ Все рефералы подписаны. Выберите VIP-канал для первой ссылки в этом цикле.",
                    reply_markup=kb,
                )
                bot.answer_callback_query(call.id, "Выберите канал", False)
            else:
                bot.answer_callback_query(
                    call.id,
                    f"Нужно набрать друзей: {ref_count} / {required}",
                    True,
                )
        elif len(owned) == 1:
            ok, err = issue_vip_access(
                uid,
                call.from_user.full_name or str(uid),
                owned[0],
                reset_refs=False,
                notify_link=True,
            )
            if ok:
                bot.answer_callback_query(call.id, "Новая ссылка отправлена.", True)
            else:
                bot.answer_callback_query(call.id, err or "Ошибка", True)
        else:
            kb = types.InlineKeyboardMarkup()
            con = sqlite3.connect("./data/vip.sql")
            cur = con.cursor()
            for vid in owned:
                row = cur.execute("SELECT name FROM vip WHERE id=?", (vid,)).fetchone()
                label = row[0] if row else str(vid)
                kb.add(types.InlineKeyboardButton(text=f"🔗 {label}", callback_data=f"vipresend:{vid}"))
            con.close()
            bot.send_message(
                uid,
                "✅ Выберите VIP-канал — пришлём новую персональную ссылку.",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id, "Выберите канал", False)


    elif call.data.startswith('admin:'):
        if call.from_user.id not in [a[0] for a in get_admin()]:
            bot.answer_callback_query(call.id, "⛔ Нет доступа", True)
            return

        if call.data == 'admin:users:search':
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="< Назад", callback_data="admin:users"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=(
                    "🔍 <b>Поиск по ID</b>\n\n"
                    "Отправь <b>числовой Telegram ID</b> следующим сообщением.\n"
                    "<i>Увидишь рефералов, VIP и реферера.</i>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
            waiting_for_user_lookup[call.from_user.id] = True
            bot.register_next_step_handler(call.message, admin_user_lookup)
            return

        if call.data.startswith('admin:lookup:'):
            try:
                target_id = int(call.data.split(':')[2])
            except Exception:
                bot.answer_callback_query(call.id, "Неверный ID", True)
                return

            class _M:
                pass

            m = _M()
            m.from_user = call.from_user
            m.chat = call.message.chat
            m.text = str(target_id)
            admin_user_lookup(m)
            bot.answer_callback_query(call.id, "Обновлено", False)
            return

        if call.data in ("admin:cfg:hub", "admin:cfg:refresh"):
            txt, ckb = build_cfg_hub_content()
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=txt,
                parse_mode="HTML",
                reply_markup=ckb,
            )
            bot.answer_callback_query(call.id)
            return

        if call.data == "admin:cfg:sub:notify":
            txt, nkb = build_cfg_notify_content()
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=txt,
                parse_mode="HTML",
                reply_markup=nkb,
            )
            bot.answer_callback_query(call.id)
            return

        if call.data == "admin:cfg:tvip":
            set_vip_invite_use_join_request(not get_vip_invite_use_join_request())
            bot.answer_callback_query(call.id, "Режим переключён", False)
            txt, ckb = build_cfg_hub_content()
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=txt,
                parse_mode="HTML",
                reply_markup=ckb,
            )
            return

        if call.data.startswith("admin:cfg:e:"):
            try:
                idx = int(call.data.split(":")[3])
                key = CFG_MENU[idx][0]
            except Exception:
                bot.answer_callback_query(call.id, "Ошибка", True)
                return
            waiting_for_cfg_key[call.from_user.id] = key
            cur_val = get_app_setting(key) or "—"
            bot.send_message(
                call.message.chat.id,
                f"{CFG_HINTS.get(key, 'Введите значение:')}\n\n"
                f"Текущее: <code>{cur_val}</code>\n\n"
                f"<i>/cancel — отмена</i>",
                parse_mode="HTML",
            )
            bot.register_next_step_handler_by_chat_id(call.message.chat.id, admin_cfg_value_step)
            bot.answer_callback_query(call.id)
            return

        if call.data == "admin:ad_kb":
            if call.from_user.id not in last_message:
                bot.answer_callback_query(call.id, "Сначала создайте текст рассылки", True)
                return
            waiting_for_ad_kb[call.from_user.id] = True
            bot.answer_callback_query(call.id)
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="◀️ Отмена", callback_data="admin:ad_cancel_kb"))
            bot.send_message(
                call.message.chat.id,
                (
                    "🔘 <b>Кнопки под рассылкой</b>\n\n"
                    "Каждая <b>строка</b> — ряд кнопок.\n"
                    "В одной строке несколько кнопок — через <code>|</code>.\n\n"
                    "<code>Название - url:https://t.me/...</code>\n"
                    "<code>Название - call:текст во всплывающем окне</code>\n\n"
                    "Отправьте <b>одним сообщением</b> весь блок.\n"
                    "Пустое сообщение — удалить все кнопки.\n\n"
                    "<i>/cancel — отмена</i>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.register_next_step_handler_by_chat_id(call.message.chat.id, ad_collect_buttons_step)
            return

        if call.data == "admin:ad_cancel_kb":
            waiting_for_ad_kb.pop(call.from_user.id, None)
            bot.answer_callback_query(call.id, "Отменено", False)
            if call.from_user.id in last_message:
                send_mailing_action_keyboard(call.message.chat.id, call.from_user.id)
            return

        if call.data == "admin:users:gift":
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:users"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=(
                    "🎁 <b>Выдача VIP</b>\n\n"
                    "Отправьте <b>числовой Telegram ID</b> пользователя (следующим сообщением).\n"
                    "<i>Пользователь должен уже быть в базе бота.</i>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
            waiting_for_gift_target[call.from_user.id] = True
            bot.register_next_step_handler_by_chat_id(call.message.chat.id, admin_gift_id_step)
            bot.answer_callback_query(call.id)
            return

        if call.data.startswith("admin:giftvip:"):
            parts = call.data.split(":")
            try:
                target_uid = int(parts[2])
                vip_pick = int(parts[3])
            except Exception:
                bot.answer_callback_query(call.id, "Ошибка данных", True)
                return
            u = get_user(target_uid)
            if not u:
                bot.answer_callback_query(call.id, "Нет в базе", True)
                return
            fn = u[1] if u else str(target_uid)
            ok, err = issue_vip_access(target_uid, fn, vip_pick, reset_refs=False, notify_link=True)
            if ok:
                bot.answer_callback_query(call.id, "VIP выдан", False)
            else:
                bot.answer_callback_query(call.id, err or "Ошибка", True)
            return

        if call.data == "admin:settings:edit_vip":
            kb = types.InlineKeyboardMarkup()
            for vid, name, _lk, _cid in get_all_vip_rows():
                kb.add(
                    types.InlineKeyboardButton(
                        text=f"✏️ {name}"[:64],
                        callback_data=f"admin:vipm:{vid}",
                    )
                )
            kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:settings"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text="🏆 <b>VIP-каналы</b>\n\nВыберите запись для редактирования:",
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        if call.data.startswith("admin:vipm:"):
            try:
                vid = int(call.data.split(":")[2])
            except Exception:
                bot.answer_callback_query(call.id, "Ошибка", True)
                return
            row = get_vip_row(vid)
            if not row:
                bot.answer_callback_query(call.id, "Не найдено", True)
                return
            _i, vname, vlink, vcid = row
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(text="📝 Название", callback_data=f"admin:vipf:{vid}:name"),
                types.InlineKeyboardButton(text="🔗 Ссылка", callback_data=f"admin:vipf:{vid}:link"),
            )
            kb.add(types.InlineKeyboardButton(text="🆔 channel_id", callback_data=f"admin:vipf:{vid}:channel_id"))
            kb.add(types.InlineKeyboardButton(text="◀️ К списку VIP", callback_data="admin:settings:edit_vip"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=(
                    f"✏️ <b>{vname}</b>\n\n"
                    f"🔗 <code>{vlink or '—'}</code>\n"
                    f"🆔 channel_id: <code>{vcid or '—'}</code>\n\n"
                    f"<i>channel_id — ID канала (отрицательное число), нужен боту для инвайтов.</i>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        if call.data.startswith("admin:vipf:"):
            parts = call.data.split(":")
            try:
                vid = int(parts[2])
                field = parts[3]
            except Exception:
                bot.answer_callback_query(call.id, "Ошибка", True)
                return
            if field not in ("name", "link", "channel_id"):
                bot.answer_callback_query(call.id, "Поле неизвестно", True)
                return
            labels = {"name": "название", "link": "ссылку", "channel_id": "channel_id (число)"}
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="◀️ Отмена", callback_data=f"admin:vipm:{vid}"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"✍️ Введите новое значение поля «<b>{labels[field]}</b>»:",
                parse_mode="HTML",
                reply_markup=kb,
            )
            waiting_for_vip_edit[call.from_user.id] = (vid, field)
            bot.register_next_step_handler_by_chat_id(call.message.chat.id, admin_vip_value_step)
            bot.answer_callback_query(call.id)
            return

        if call.data == "admin:edit_admin":
            rows = get_admin()
            lines = "\n".join(f"• <code>{r[0]}</code>" for r in rows) or "—"
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="➕ Добавить ID", callback_data="admin:admin_add"))
            for r in rows:
                kb.add(
                    types.InlineKeyboardButton(
                        text=f"🗑 Убрать {r[0]}",
                        callback_data=f"admin:admin_del:{r[0]}",
                    )
                )
            kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:settings"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"👑 <b>Администраторы</b>\n\n{lines}\n\n<i>Добавление — следующим сообщением после нажатия «Добавить».</i>",
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        if call.data == "admin:admin_add":
            waiting_for_admin_add[call.from_user.id] = True
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:edit_admin"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text="➕ Отправьте <b>числовой ID</b> нового администратора:",
                parse_mode="HTML",
                reply_markup=kb,
            )
            bot.register_next_step_handler_by_chat_id(call.message.chat.id, admin_add_id_step)
            bot.answer_callback_query(call.id)
            return

        if call.data.startswith("admin:admin_del:"):
            try:
                aid = int(call.data.split(":")[2])
            except Exception:
                bot.answer_callback_query(call.id, "Ошибка", True)
                return
            if aid == call.from_user.id:
                bot.answer_callback_query(call.id, "Нельзя удалить себя", True)
                return
            if delete_admin_row(aid):
                bot.answer_callback_query(call.id, "Удалён", False)
            else:
                bot.answer_callback_query(call.id, "Не найден", True)
            rows = get_admin()
            lines = "\n".join(f"• <code>{r[0]}</code>" for r in rows) or "—"
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="➕ Добавить ID", callback_data="admin:admin_add"))
            for r in rows:
                kb.add(
                    types.InlineKeyboardButton(
                        text=f"🗑 Убрать {r[0]}",
                        callback_data=f"admin:admin_del:{r[0]}",
                    )
                )
            kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:settings"))
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"👑 <b>Администраторы</b>\n\n{lines}\n\n<i>Добавление — следующим сообщением после нажатия «Добавить».</i>",
                parse_mode="HTML",
                reply_markup=kb,
            )
            return

        arg = call.data.split(":")[1]

        if arg == "menu":
            if call.from_user.id in waiting_for_ad:
                del waiting_for_ad[call.from_user.id]
            waiting_for_ad_kb.pop(call.from_user.id, None)
            waiting_for_cfg_key.pop(call.from_user.id, None)

            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(text="📈 Статистика", callback_data="admin:stats"),
                types.InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users"),
            )
            kb.add(
                types.InlineKeyboardButton(text="✉️ Рассылка", callback_data="admin:ad"),
                types.InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin:settings"),
            )

            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=(
                    "🛡 <b>Главное меню</b>\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "📈 <b>Статистика</b> — аудитория, рефералы, неделя\n"
                    "👥 <b>Пользователи</b> — поиск, выдача VIP\n"
                    "✉️ <b>Рассылка</b> — пост + кнопки\n"
                    "⚙️ <b>Настройки</b> — бот, VIP, админы"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
        elif arg == 'stats':
            users = sqlite3.connect('./data/users.sql').cursor().execute('SELECT COUNT(id) FROM users').fetchone()[0]
            referals = sqlite3.connect('./data/users.sql').cursor().execute('SELECT COUNT(ref_id) FROM users WHERE ref_id != "None"').fetchone()[0]

            conn = sqlite3.connect('./data/users.sql')
            cursor = conn.cursor()

            today = datetime.now()
            current_weekday = today.weekday()

            days_of_week = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
            week_stats = [0] * 7

            for i in range(current_weekday + 1):
                day_date = (today - timedelta(days=current_weekday - i)).strftime("%Y-%m-%d")
                cursor.execute('SELECT COUNT(*) FROM users WHERE DATE(data) = ?', (day_date,))
                count = cursor.fetchone()[0]
                week_stats[i] = count

            cursor.execute('SELECT COUNT(*) FROM users WHERE DATE(data) = ?', (today.strftime("%Y-%m-%d"),))
            today_users = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM users WHERE DATE(data) = ?', ((today - timedelta(days=1)).strftime("%Y-%m-%d"),))
            yesterday_users = cursor.fetchone()[0]

            growth = ((today_users - yesterday_users) / yesterday_users * 100) if yesterday_users > 0 else 0

            cursor.execute('''
                SELECT u.name, COUNT(*) as ref_count 
                FROM users r 
                JOIN users u ON r.ref_id = u.id 
                WHERE r.ref_id != "None" 
                GROUP BY r.ref_id 
                ORDER BY ref_count DESC 
                LIMIT 5
            ''')
            top_referers = cursor.fetchall()

            cursor.execute('''
                SELECT name, ref_count 
                FROM users 
                WHERE ref_count > 0 
                ORDER BY ref_count DESC 
                LIMIT 5
            ''')
            top_referals = cursor.fetchall()

            conn.close()

            week_stats_text = ""
            for i, day in enumerate(days_of_week):
                week_stats_text += f"├ {day}: +{week_stats[i]}\n"

            top_referers_text = ""
            for i, (name, count) in enumerate(top_referers):
                top_referers_text += f"{i+1}. {name}: {count} реф.\n"

            top_referals_text = ""
            for i, (name, count) in enumerate(top_referals):
                top_referals_text += f"{i+1}. {name}: {count} реф.\n"

            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:stats"),
                types.InlineKeyboardButton(text="◀️ Меню", callback_data="admin:menu"),
            )

            final_text = (
                f"✦ ✦ ✦\n"
                f"📊 <b>АНАЛИТИКА</b>\n"
                f"<i>🕐 {_fmt_timestamp()}</i>\n"
                f"✦ ✦ ✦\n\n"
                f"👥 <b>Аудитория</b>\n"
                f"<blockquote>всего: <code>{users}</code>\n"
                f"сегодня: <code>+{today_users}</code>\n"
                f"вчера: <code>+{yesterday_users}</code></blockquote>\n\n"
                f"🏆 <b>Рефералы</b>\n"
                f"<blockquote>связей ref: <code>{referals}</code>\n"
                f"конверсия: <code>{round((referals/users)*100, 1) if users > 0 else 0}%</code></blockquote>\n\n"
                f"📈 <b>Динамика</b>\n"
                f"<blockquote>рост день к дню: <code>{growth:+.1f}%</code>\n"
                f"(сегодня vs вчера)</blockquote>\n\n"
                f"📅 <b>Неделя по дням</b>\n"
                f"<blockquote>{week_stats_text}└ Σ за неделю: <code>{sum(week_stats)}</code></blockquote>\n\n"
                f"🏅 <b>Топ по счётчику в профиле</b>\n"
                f"<blockquote>{top_referals_text or '—'}</blockquote>\n\n"
                f"🌟 <b>Топ пригласителей (факт в базе)</b>\n"
                f"<blockquote>{top_referers_text or '—'}</blockquote>"
            )

            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=final_text, reply_markup=kb, parse_mode="HTML")
        elif arg == 'users':
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(text="🔍 Найти по ID", callback_data="admin:users:search"),
                types.InlineKeyboardButton(text="🎁 Выдать VIP", callback_data="admin:users:gift"),
            )
            kb.add(types.InlineKeyboardButton(text="◀️ В меню", callback_data="admin:menu"))

            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=(
                    "👥 <b>Пользователи</b>\n"
                    "━━━━━━━━━━━━━━\n"
                    "🔍 Полная карточка по Telegram ID\n"
                    "🎁 Выдать VIP без рефералов\n\n"
                    "<i>Быстрый доступ к данным и ручной бонус.</i>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
        elif arg == 'ad':
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu"))
            bot.edit_message_text(
                message_id=call.message.message_id,
                chat_id=call.message.chat.id,
                text=(
                    "✉️ <b>Рассылка</b>\n"
                    "━━━━━━━━━━━━━━\n"
                    "1️⃣ Отправь <b>текст</b> или <b>фото + подпись</b>\n"
                    "2️⃣ При желании — <b>Добавить кнопки</b> вторым сообщением\n"
                    "3️⃣ <b>Отправить всем</b> — старт\n\n"
                    "<blockquote>Лайфхак: в том же тексте можно вставить\n"
                    "<code>---KEYBOARD---</code>\n"
                    "и ниже строки кнопок — без отдельного шага.</blockquote>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )

            waiting_for_ad[call.from_user.id] = True
            bot.register_next_step_handler_by_chat_id(call.message.chat.id, get_ad_text)
        elif arg == "ad_send":
            msg = last_message[call.from_user.id]
            markup = build_broadcast_markup(msg.get("keyboard_rows") or [])

            for i in sqlite3.connect('./data/users.sql').cursor().execute('SELECT id FROM users').fetchall():
                try:
                    global ad_count
                    if msg['content_type'] == 'photo':
                        bot.send_photo(
                            i[0],
                            msg['photo'],
                            caption=msg.get('caption'),
                            caption_entities=msg.get('caption_entities'),
                            reply_markup=markup,
                        )
                    else:
                        bot.send_message(
                            i[0],
                            msg['text'],
                            entities=msg.get('entities'),
                            reply_markup=markup,
                        )
                    ad_count += 1
                except Exception as e:
                    global fail_count
                    fail_count += 1
                    print(f'Неудалось отправить сообщение пользователь ID: {i}\n\n{e}')

            bot.send_message(
                call.from_user.id,
                f"✅ <b>Рассылка завершена</b>\n"
                f"<i>🕐 {_fmt_timestamp()}</i>\n\n"
                f"<blockquote>✔️ доставлено: <code>{ad_count}</code>\n"
                f"✖️ ошибок: <code>{fail_count}</code></blockquote>",
                parse_mode="HTML",
            )
            fail_count = 0
            ad_count = 0

        elif call.data == 'admin:settings':
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(text="🤖 Параметры бота", callback_data="admin:cfg:hub"),
            )
            kb.add(
                types.InlineKeyboardButton(text="🏆 VIP-каналы", callback_data="admin:settings:edit_vip"),
                types.InlineKeyboardButton(text="👑 Админы", callback_data="admin:edit_admin"),
            )
            kb.add(types.InlineKeyboardButton(text="◀️ В меню", callback_data="admin:menu"))

            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=(
                    "⚙️ <b>Настройки</b>\n"
                    "━━━━━━━━━━━━━━\n"
                    "🤖 <b>Параметры</b> — рефералы, <b>интервал проверки VIP</b>, лог-чат, темы, тип ссылки\n"
                    "🏆 <b>VIP</b> — карточки каналов в БД\n"
                    "👑 <b>Админы</b> — кто видит эту панель\n\n"
                    "<i>Всё критичное — в «Параметры бота».</i>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )

def get_ad_text(message):
    if message.from_user.id not in waiting_for_ad:
        return

    if message.text and message.text.startswith("/"):
        del waiting_for_ad[message.from_user.id]
        if message.text.startswith("/start"):
            command_start(message)
        elif message.text.startswith("/admin") or message.text.startswith("/panel"):
            command_admin(message)
        return

    del waiting_for_ad[message.from_user.id]

    text_main, kb_from_text = split_broadcast_text(message.text)
    cap = message.caption or ""
    cap_main, kb_from_cap = split_broadcast_text(cap)

    keyboard_rows = None
    if kb_from_text:
        keyboard_rows = parse_keyboard_spec(kb_from_text)
    elif kb_from_cap:
        keyboard_rows = parse_keyboard_spec(kb_from_cap)

    use_text_entities = message.entities if not kb_from_text else None
    use_cap_entities = message.caption_entities if (message.photo and not kb_from_cap) else None
    if message.photo and kb_from_cap:
        use_cap_entities = None

    last_message[message.from_user.id] = {
        "text": text_main if message.text else message.text,
        "photo": message.photo[-1].file_id if message.photo else None,
        "entities": use_text_entities,
        "caption": cap_main if message.photo else None,
        "caption_entities": use_cap_entities,
        "content_type": message.content_type,
        "keyboard_rows": keyboard_rows or [],
    }

    msg = last_message[message.from_user.id]

    preview_markup = build_broadcast_markup(msg.get("keyboard_rows") or [])

    if msg["content_type"] == "photo":
        bot.send_photo(
            message.chat.id,
            msg["photo"],
            caption=msg.get("caption"),
            caption_entities=msg.get("caption_entities"),
        )
    else:
        bot.send_message(
            message.chat.id,
            msg["text"] or "",
            entities=msg.get("entities"),
        )
    if preview_markup:
        bot.send_message(
            message.chat.id,
            "👆 <b>Превью кнопок</b> — так увидят получатели.",
            parse_mode="HTML",
            reply_markup=preview_markup,
        )

    send_mailing_action_keyboard(message.chat.id, message.from_user.id)


def admin_cfg_value_step(message):
    uid = message.from_user.id
    if uid not in waiting_for_cfg_key:
        return
    key = waiting_for_cfg_key.pop(uid)
    raw = (message.text or "").strip()
    if raw == "/cancel":
        bot.send_message(message.chat.id, "Без изменений.")
        return
    try:
        if key == "ref_count_required":
            set_ref_count_required(max(1, int(raw)))
        elif key == "vip_reconcile_interval":
            set_vip_reconcile_interval(raw)
        elif key.startswith("notify_"):
            if raw in ("-", "—", "none", "None", ".", "clear"):
                set_app_setting(key, "")
            else:
                set_app_setting(key, str(int(raw)))
        else:
            set_app_setting(key, raw)
    except ValueError as e:
        if key == "vip_reconcile_interval":
            bot.send_message(
                message.chat.id,
                f"❌ Неверный интервал. Примеры: <code>30s</code> <code>5m</code> <code>1h</code> <code>3d</code>\n"
                f"<i>{e}</i>",
                parse_mode="HTML",
            )
        else:
            bot.send_message(message.chat.id, "Нужно целое число. Откройте панель и попробуйте снова.")
        return
    bot.send_message(
        message.chat.id,
        "✅ <b>Сохранено.</b> Параметры: ⚙️ → 🤖 Параметры бота.",
        parse_mode="HTML",
    )


def ad_collect_buttons_step(message):
    uid = message.from_user.id
    if not waiting_for_ad_kb.get(uid):
        return
    waiting_for_ad_kb.pop(uid, None)
    raw = (message.text or "").strip()
    if raw == "/cancel":
        bot.send_message(message.chat.id, "Ок.")
        if uid in last_message:
            send_mailing_action_keyboard(message.chat.id, uid)
        return
    if uid not in last_message:
        bot.send_message(message.chat.id, "Черновик устарел — начните рассылку заново.")
        return
    last_message[uid]["keyboard_rows"] = parse_keyboard_spec(raw) if raw else []
    n = len(last_message[uid]["keyboard_rows"])
    bot.send_message(message.chat.id, f"✅ Кнопки обновлены: <b>{n}</b> ряд(ов).", parse_mode="HTML")
    send_mailing_action_keyboard(message.chat.id, uid)


def admin_gift_id_step(message):
    if message.from_user.id not in waiting_for_gift_target:
        return
    del waiting_for_gift_target[message.from_user.id]
    if message.text and message.text.startswith("/"):
        return
    try:
        tid = int((message.text or "").strip())
    except Exception:
        bot.send_message(message.chat.id, "Нужен числовой Telegram ID.")
        return
    if not get_user(tid):
        bot.send_message(message.chat.id, "Пользователь не найден в базе — пусть сначала нажмёт /start.")
        return
    kb = types.InlineKeyboardMarkup()
    for vid, name, _lk, _cid in get_all_vip_rows():
        kb.add(
            types.InlineKeyboardButton(
                text=f"🎁 {name}"[:64],
                callback_data=f"admin:giftvip:{tid}:{vid}",
            )
        )
    kb.add(types.InlineKeyboardButton(text="◀️ Назад", callback_data="admin:users"))
    bot.send_message(
        message.chat.id,
        f"Пользователь <code>{tid}</code> — выберите VIP:",
        parse_mode="HTML",
        reply_markup=kb,
    )


def admin_vip_value_step(message):
    aid = message.from_user.id
    if aid not in waiting_for_vip_edit:
        return
    vid, field = waiting_for_vip_edit.pop(aid)
    if message.text and message.text.startswith("/"):
        waiting_for_vip_edit[aid] = (vid, field)
        return
    raw = (message.text or "").strip()
    try:
        if field == "channel_id":
            val = int(raw)
        else:
            val = raw
        update_vip_field(vid, field, val)
    except Exception as e:
        bot.send_message(message.chat.id, f"Ошибка: {e}")
        return
    bot.send_message(message.chat.id, "✅ Сохранено. Откройте настройки VIP снова через панель.")


def admin_add_id_step(message):
    if message.from_user.id not in waiting_for_admin_add:
        return
    del waiting_for_admin_add[message.from_user.id]
    if message.text and message.text.startswith("/"):
        return
    try:
        new_id = int((message.text or "").strip())
    except Exception:
        bot.send_message(message.chat.id, "Нужен числовой ID.")
        return
    if add_admin_row(new_id):
        bot.send_message(message.chat.id, f"✅ Админ <code>{new_id}</code> добавлен.", parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, "Уже в списке или ошибка записи.")


def admin_user_lookup(message):
    if message.from_user.id not in waiting_for_user_lookup:
        return
    del waiting_for_user_lookup[message.from_user.id]

    try:
        target_id = int((message.text or "").strip())
    except Exception:
        bot.send_message(message.chat.id, "ID должен быть числом.")
        return

    user = sqlite3.connect('./data/users.sql').cursor().execute('SELECT id, name, ref_count, ref_id, data FROM users WHERE id = ?', (target_id,)).fetchone()
    if not user:
        bot.send_message(message.chat.id, "Пользователь не найден.")
        return

    uid, name, ref_count, ref_id, reg_date = user
    referrer = None
    if ref_id and str(ref_id) != "None":
        referrer = sqlite3.connect('./data/users.sql').cursor().execute('SELECT id, name FROM users WHERE id = ?', (int(ref_id),)).fetchone()

    referrals = get_referrals(int(uid))
    credited = [r for r in referrals if int(r[2]) == 1]
    unsub = [r for r in credited if int(r[3]) == 0]

    grants = get_vip_grants(int(uid))
    vcon = sqlite3.connect("./data/vip.sql")
    vcur = vcon.cursor()
    vip_lines = []
    for g_vip_id, g_active in grants:
        vrow = vcur.execute("SELECT name FROM vip WHERE id=?", (int(g_vip_id),)).fetchone()
        nm = vrow[0] if vrow else str(g_vip_id)
        st = "активен" if int(g_active) == 1 else "приостановлен"
        vip_lines.append(f"• <b>{nm}</b> · <code>{g_vip_id}</code> — {st}")
    vcon.close()
    vip_text = "\n".join(vip_lines) if vip_lines else "—"

    referrer_text = "нет"
    if referrer:
        referrer_text = f'{referrer[1]} [ID: {referrer[0]}]'

    lines = []
    for rid, rname, credited_flag, subscribed_flag in credited[:20]:
        lines.append(f'• {rname} [ID: {rid}] — {"✅" if int(subscribed_flag)==1 else "❌"}')

    referral_list = "\n".join(lines) if lines else "—"

    text = (
        f"╔══════════════════════════╗\n"
        f"║  📇 <b>КАРТОЧКА ЮЗЕРА</b>  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"{_fmt_user_block(int(uid), name)}\n"
        f"📅 Регистрация: <code>{reg_date}</code>\n\n"
        f"🎯 <b>Рефералы</b>\n"
        f"<blockquote>счётчик в users: <code>{ref_count}</code>\n"
        f"засчитано в таблице: <code>{len(credited)}</code>\n"
        f"сейчас не подписаны: <code>{len(unsub)}</code></blockquote>\n\n"
        f"🔗 <b>Реферер:</b> {referrer_text}\n"
        f"🎟 <b>VIP (привязки к каналам)</b>\n<blockquote>{vip_text}</blockquote>\n\n"
        f"👥 <b>Рефералы (до 20)</b>\n"
        f"<blockquote>{referral_list}</blockquote>\n\n"
        f"<i>🕐 {_fmt_timestamp()}</i>"
    )

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(text="🔄 Обновить", callback_data=f"admin:lookup:{uid}"),
        types.InlineKeyboardButton(text="◀️ К юзерам", callback_data="admin:users"),
    )
    bot.send_message(message.chat.id, text, parse_mode="HTML", reply_markup=kb)


def save_message(message):
    last_message[message.chat.id] = {
        'text': message.text,
        'entities': message.entities,
        'caption': message.caption,
        'caption_entities': message.caption_entities,
        'content_type': message.content_type
    }
    bot.send_message(message.chat.id, "Сообщение сохранено. Используйте /repeat")

def check_subscription(user_id):
    con = sqlite3.connect('./data/admin.sql')
    cur = con.cursor()
    result = cur.execute(f'SELECT id FROM admin WHERE id = {user_id}').fetchone()
    con.close()

    if result:
        return True
    
    try:
        con = sqlite3.connect('./data/sub.sql')
        cur = con.cursor()
        channels = cur.execute('SELECT id FROM channel').fetchall()
        con.close()
        
        need_to_check = False
        
        for row in channels:
            cid = int(row[0])
            need_to_check = True
            try:
                member = bot.get_chat_member(cid, user_id)
                if member.status not in ['member', 'administrator', 'creator']:
                    return False
            except Exception:
                return False

        return True
    except Exception:
        return False

def keyboard_sub(type, user_id, message_id=None, ref_id=None):
    print(f"Реферал {ref_id}")
    ref_id = f'_{ref_id}' if ref_id else ''
    if type == 1:
        con = sqlite3.connect('./data/sub.sql')
        cur = con.cursor()
        cur.execute('SELECT id, name, link FROM channel')
        kb = types.InlineKeyboardMarkup()
        for id, name, link in cur.fetchall():
            try:
                member = bot.get_chat_member(id, user_id)
                if not member.status in ['member', 'administrator', 'creator']:
                    kb.row(types.InlineKeyboardButton(text=f'{name}', url=link))
            except:
                print(id, name, link)
        kb.row(types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data=f"check_sub{ref_id}"))
        bot.send_message(
            user_id,
            "🎓 <b>Почти всё готово!</b>\n\n"
            "Подпишись на каналы ниже, затем нажми <b>«Проверить подписку»</b>.\n"
            "<i>Без этого бот не сможет выдать материалы и VIP.</i>",
            parse_mode="HTML",
            reply_markup=kb,
        )
    else:
        con = sqlite3.connect('./data/sub.sql')
        cur = con.cursor()
        cur.execute('SELECT id, name, link FROM channel')
        kb = types.InlineKeyboardMarkup()
        for id, name, link in cur.fetchall():
            try:
                member = bot.get_chat_member(id, user_id)
                if member.status in ['member', 'administrator', 'creator']:
                    kb.row(types.InlineKeyboardButton(text=f'{name}', url=link))
            except Exception:
                print(id, name, link)
        kb.row(types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data=f"check_sub{ref_id}"))
        bot.edit_message_text(
            chat_id=user_id,
            message_id=message_id,
            text=(
                "🎓 <b>Проверь подписки</b>\n\n"
                "Зайди во все каналы из списка и нажми <b>«Проверить подписку»</b>."
            ),
            parse_mode="HTML",
            reply_markup=kb,
        )


def _vip_managed_channel_ids():
    return {int(r[3]) for r in get_all_vip_rows() if r[3] is not None}


def _handle_vip_join_verdict(chat_id: int, joined_user_id: int, invite_url: str | None):
    if not invite_url:
        return
    pending = get_vip_invite_pending(invite_url)
    if not pending:
        return
    _link, expected_uid, channel_id, vip_id, req_name = pending
    vrow = get_vip_row(int(vip_id))
    vname = vrow[1] if vrow else str(vip_id)
    if joined_user_id == int(expected_uid):
        delete_vip_invite_pending(invite_url)
        send_topic(
            bot,
            "NOTIFY_TOPIC_VIP_JOIN_OK",
            f"{_log_banner('✅', 'VIP: вступление по инвайту (прямой вход)')}"
            f"{_fmt_user_block(joined_user_id)}\n"
            f"{_vip_notify_lines(int(vip_id), vrow)}\n"
            f"📢 chat_id: <code>{chat_id}</code>\n"
            f"<blockquote>Инвайт (обрезан):\n<code>{invite_url[:72]}{'…' if len(invite_url) > 72 else ''}</code></blockquote>",
        )
        return
    delete_vip_invite_pending(invite_url)
    kick_user_from_channel(int(channel_id), joined_user_id)
    send_topic(
        bot,
        "NOTIFY_TOPIC_VIP_INTRUDER",
        f"{_log_banner('⛔', 'VIP: чужой вошёл по ссылке (прямой вход)')}"
        f"<b>Нарушитель</b>\n{_fmt_user_block(joined_user_id)}\n\n"
        f"<b>Ожидался владелец ссылки</b>\n{_fmt_user_block(int(expected_uid), req_name)}\n\n"
        f"{_vip_notify_lines(int(vip_id), vrow)}\n"
        f"📢 channel: <code>{channel_id}</code>\n"
        f"<blockquote>Кик из канала + выдача <b>новой</b> ссылки владельцу.</blockquote>",
    )
    try:
        exp_chat = bot.get_chat(int(expected_uid))
        exp_name = exp_chat.full_name or req_name or str(expected_uid)
    except Exception:
        exp_name = req_name or str(expected_uid)
    issue_vip_access(int(expected_uid), exp_name, int(vip_id), reset_refs=False, notify_link=True)


@bot.chat_join_request_handler(func=lambda r: r.chat.id in _vip_managed_channel_ids())
def on_vip_join_request(req):
    if not req.invite_link:
        return
    url = req.invite_link.invite_link
    pending = get_vip_invite_pending(url)
    if not pending:
        return
    _link, expected_uid, channel_id, vip_id, req_name = pending
    chat_id = req.chat.id
    uid = req.from_user.id
    vrow = get_vip_row(int(vip_id))
    vname = vrow[1] if vrow else str(vip_id)
    if uid == int(expected_uid):
        try:
            bot.approve_chat_join_request(chat_id, uid)
        except Exception as e:
            print(f"approve_chat_join_request: {e}")
        delete_vip_invite_pending(url)
        nm = getattr(req.from_user, "full_name", None) or str(uid)
        send_topic(
            bot,
            "NOTIFY_TOPIC_VIP_JOIN_OK",
            f"{_log_banner('✅', 'VIP: заявка одобрена (авто)')}"
            f"{_fmt_user_block(uid, nm)}\n"
            f"{_vip_notify_lines(int(vip_id), vrow)}\n"
            f"📢 chat_id: <code>{chat_id}</code>\n"
            f"<blockquote>Заявка принята ботом. Инвайт снят с учёта.</blockquote>",
        )
    else:
        try:
            bot.decline_chat_join_request(chat_id, uid)
        except Exception as e:
            print(f"decline_chat_join_request: {e}")
        try:
            bot.ban_chat_member(chat_id, uid)
        except Exception as e:
            print(f"ban_chat_member (intruder): {e}")
        bad_nm = getattr(req.from_user, "full_name", None) or str(uid)
        send_topic(
            bot,
            "NOTIFY_TOPIC_VIP_INTRUDER",
            f"{_log_banner('⛔', 'VIP: чужая заявка по ссылке')}"
            f"<b>Заявитель (отклонён + бан)</b>\n{_fmt_user_block(uid, bad_nm)}\n\n"
            f"<b>Владелец ссылки</b>\n{_fmt_user_block(int(expected_uid), req_name)}\n\n"
            f"{_vip_notify_lines(int(vip_id), vrow)}\n"
            f"📢 chat_id: <code>{chat_id}</code>\n"
            f"<blockquote>Заявка отклонена. Ссылка владельца <b>сохранена</b> — повторная выдача не требуется.</blockquote>",
        )
        try:
            bot.send_message(
                int(expected_uid),
                "🛡 <b>Сработала защита VIP</b>\n\n"
                "Кто-то подал <b>заявку</b> по вашей персональной ссылке.\n"
                "Бот отклонил заявку и <b>заблокировал</b> этого пользователя в канале.\n\n"
                "🔐 <b>Доступ к каналу имеете только вы!</b>\n"
                "<i>Ваша ссылка действует дальше — можно подать заявку снова самому.</i>",
                parse_mode="HTML",
            )
        except Exception as e:
            print(f"notify expected user intruder: {e}")


@bot.chat_member_handler(func=lambda u: u.chat.id in _vip_managed_channel_ids())
def on_vip_chat_member(ch):
    new = ch.new_chat_member
    if new.status not in ("member", "administrator", "creator"):
        return
    inv = ch.invite_link
    url = inv.invite_link if inv else None
    _handle_vip_join_verdict(ch.chat.id, new.user.id, url)


threading.Thread(target=background_reconcile_loop, daemon=True).start()
bot.infinity_polling(
    allowed_updates=[
        "message",
        "callback_query",
        "chat_member",
        "chat_join_request",
    ],
)