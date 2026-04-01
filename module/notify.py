"""Уведомления в супергруппу с топиками — параметры из БД (app_kv)."""
from module.app_settings import get_app_setting_int

# Имя, как в main.py → ключ в app_kv
TOPIC_KEYS = {
    "NOTIFY_TOPIC_NEW_USER": "notify_topic_new_user",
    "NOTIFY_TOPIC_VIP_LINK_ISSUED": "notify_topic_vip_link_issued",
    "NOTIFY_TOPIC_VIP_JOIN_OK": "notify_topic_vip_join_ok",
    "NOTIFY_TOPIC_VIP_INTRUDER": "notify_topic_vip_intruder",
    "NOTIFY_TOPIC_VIP_ACCESS_REVOKED": "notify_topic_vip_access_revoked",
    "NOTIFY_TOPIC_REFERRAL_UNSUB": "notify_topic_referral_unsub",
}


def send_topic(bot, topic_setting: str, text: str, parse_mode: str = "HTML") -> None:
    chat_id = get_app_setting_int("notify_log_chat_id")
    if chat_id is None:
        return
    db_key = TOPIC_KEYS.get(topic_setting)
    if not db_key:
        print(f"[notify] unknown topic {topic_setting}")
        return
    thread_id = get_app_setting_int(db_key)
    try:
        kwargs = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
        if thread_id is not None:
            kwargs["message_thread_id"] = int(thread_id)
        bot.send_message(**kwargs)
    except Exception as e:
        print(f"[notify] {topic_setting}: {e}")
