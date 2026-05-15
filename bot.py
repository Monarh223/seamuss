import asyncio
import html
import io
import json
import logging
import os
import re
import sqlite3
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile, CallbackQuery, FSInputFile, Message, ForceReply, InlineKeyboardButton, MenuButtonCommands
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =========================================================
# CONFIG - ALL IN ONE FILE
# Secrets are loaded from Railway Variables / environment.
# Do NOT hardcode real bot tokens or payment API tokens in this file.
# =========================================================

def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int = 0) -> int:
    raw = _env_str(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float = 0.0) -> float:
    raw = _env_str(name, str(default))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env_str(name, "1" if default else "0").lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _env_int_list(name: str, default: list[int] | None = None) -> list[int]:
    raw = _env_str(name, "")
    if not raw:
        return list(default or [])
    values: list[int] = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            values.append(int(part))
        except ValueError:
            logging.warning("Invalid integer in %s: %r", name, part)
    return values


BOT_TOKEN = _env_str("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty. Set it in Railway Variables before starting the bot.")

DB_PATH = _env_str("DB_PATH", "bot.db")
BOT_USERNAME_FALLBACK = _env_str("BOT_USERNAME_FALLBACK", "esimservicexbot")

# Roles
CHIEF_ADMIN_ID = _env_int("CHIEF_ADMIN_ID", 7133092873)
BOOTSTRAP_ADMINS = _env_int_list("BOOTSTRAP_ADMINS", [626387429])
BOOTSTRAP_OPERATORS = _env_int_list("BOOTSTRAP_OPERATORS", [])

WITHDRAW_CHANNEL_ID = _env_int("WITHDRAW_CHANNEL_ID", -1003785698154)
LOG_CHANNEL_ID = _env_int("LOG_CHANNEL_ID", 0)
MIN_WITHDRAW = _env_float("MIN_WITHDRAW", 10.0)
DEFAULT_HOLD_MINUTES = _env_int("DEFAULT_HOLD_MINUTES", 15)
DEFAULT_TREASURY_BALANCE = _env_float("DEFAULT_TREASURY_BALANCE", 0.0)

# Crypto Bot / Crypto Pay API
CRYPTO_PAY_TOKEN = _env_str("CRYPTO_PAY_TOKEN")
CRYPTO_PAY_BASE_URL = _env_str("CRYPTO_PAY_BASE_URL", "https://pay.crypt.bot/api")
CRYPTO_PAY_ASSET = _env_str("CRYPTO_PAY_ASSET", "USDT")
CRYPTO_PAY_PIN_CHECK_TO_USER = _env_bool("CRYPTO_PAY_PIN_CHECK_TO_USER", False)  # True -> check pinned to telegram user

OPERATORS = {
    "mts": {"title": "МТС ГК", "price": 14.00, "command": "/mts"},
    "mtssalon": {"title": "МТС Салон", "price": 18.00, "command": "/mtssalon"},
    "bil": {"title": "Билайн ГК", "price": 14.00, "command": "/bil"},
    "bilsalon": {"title": "Билайн Салон", "price": 16.00, "command": "/bilsalon"},
    "tele2": {"title": "Tele2 ГК", "price": 13.00, "command": "/tele2"},
    "tele2salon": {"title": "Tele2 Салон", "price": 15.00, "command": "/tele2salon"},
    "sber": {"title": "Сбер", "price": 12.00, "command": "/sber"},
    "megafon": {"title": "Мегафон", "price": 10.00, "command": "/megafon"},
    "vtb": {"title": "ВТБ", "price": 20.00, "command": "/vtb"},
    "gazprom": {"title": "Газпром", "price": 22.00, "command": "/gazprom"},
    "miranda": {"title": "Миранда", "price": 15.00, "command": "/miranda"},
    "dobrosvyz": {"title": "Добросвязь", "price": 8.00, "command": "/dobrosvyz"},
}
DEFAULT_GROUP_PRICES = {
    "mts": 18.00, "mtssalon": 18.00, "bil": 17.00, "bilsalon": 17.00,
    "tele2": 0.00, "tele2salon": 0.00, "sber": 12.00, "megafon": 0.00,
    "vtb": 0.00, "gazprom": 0.00, "miranda": 15.00, "dobrosvyz": 8.00,
}
BASE_OPERATOR_KEYS = set(OPERATORS.keys())
PERMANENT_OPERATOR_CONFIG = {k: dict(v) for k, v in OPERATORS.items()}

PERMANENT_OPERATOR_PRICES = {
    "mts": {"hold": 18.0, "no_hold": 15.0},
    "mtssalon": {"hold": 18.0, "no_hold": 18.0},
    "bil": {"hold": 17.0, "no_hold": 14.0},
    "bilsalon": {"hold": 17.0, "no_hold": 16.0},
    "tele2": {"hold": 14.0, "no_hold": 13.0},
    "tele2salon": {"hold": 15.0, "no_hold": 15.0},
    "sber": {"hold": 12.0, "no_hold": 12.0},
    "megafon": {"hold": 10.0, "no_hold": 10.0},
    "vtb": {"hold": 22.0, "no_hold": 22.0},
    "gazprom": {"hold": 30.0, "no_hold": 30.0},
    "miranda": {"hold": 19.0, "no_hold": 19.0},
    "dobrosvyz": {"hold": 12.0, "no_hold": 12.0},
}

for _op_key, _prices in PERMANENT_OPERATOR_PRICES.items():

    if _op_key in PERMANENT_OPERATOR_CONFIG:
        PERMANENT_OPERATOR_CONFIG[_op_key]["price"] = float(_prices["hold"])
PERMANENT_OPERATOR_KEYS = set(PERMANENT_OPERATOR_CONFIG.keys())
GROUP_DEFAULT_PRICES_BY_MODE = {
    "hold": {k: float(v["hold"]) for k, v in PERMANENT_OPERATOR_PRICES.items()},
    "no_hold": {k: float(v["no_hold"]) for k, v in PERMANENT_OPERATOR_PRICES.items()},
}

ACTIVE_OPERATOR_KEYS = set(PERMANENT_OPERATOR_CONFIG.keys())
OPERATOR_KEY_ALIASES = {
    "mtc": "mts", "mts_premium": "mtssalon", "mtspremium": "mtssalon", "mts_salon": "mtssalon",
    "bilper": "bil", "bilsber": "bil", "bilsalon2": "bilsalon",
    "mega": "megafon", "t2": "tele2", "tele2slaon": "tele2salon", "tele2_salon": "tele2salon",
    "gaz": "gazprom", "dobro": "dobrosvyz", "dobrosvyaz": "dobrosvyz",
}

def is_operator_hidden_setting(operator_key: str) -> bool:
    try:
        return str(db.get_setting(f"operator_hidden_{normalize_operator_key(operator_key)}", "0")) == "1"
    except Exception:
        return False

def is_operator_visible(operator_key: str) -> bool:
    key = normalize_operator_key(operator_key)
    if not key or key not in OPERATORS:
        return False
    if key not in ACTIVE_OPERATOR_KEYS:
        return False
    return not is_operator_hidden_setting(key)

def visible_operator_keys() -> list[str]:
    return [k for k in OPERATORS.keys() if is_operator_visible(k)]

# =========================================================

START_BANNER = "start_banner.jpg"
PROFILE_BANNER = "profile_banner.jpg"
MY_NUMBERS_BANNER = "my_numbers_banner.jpg"
WITHDRAW_BANNER = "withdraw_banner.jpg"
MSK_OFFSET = timedelta(hours=3)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", mode="a", encoding="utf-8")])
logging.info("Railway logging enabled: stdout + bot.log")

_HANDLED_EVENT_KEYS: dict[tuple, float] = {}


def consume_event_once(*parts, ttl_seconds: int = 120) -> bool:
    now_ts = time.time()
    stale_keys = [key for key, seen_at in _HANDLED_EVENT_KEYS.items() if now_ts - seen_at > ttl_seconds]
    for key in stale_keys:
        _HANDLED_EVENT_KEYS.pop(key, None)
    key = tuple(parts)
    if key in _HANDLED_EVENT_KEYS:
        logging.warning("duplicate event skipped: %s", key)
        return False
    _HANDLED_EVENT_KEYS[key] = now_ts
    return True

def debug_workspace_rows(chat_id: int):
    try:
        rows = db.conn.execute(
            "SELECT id, chat_id, thread_id, mode, is_enabled, added_by, created_at FROM workspaces WHERE chat_id=? ORDER BY id DESC",
            (chat_id,),
        ).fetchall()
        payload = [dict(row) for row in rows]
        logging.info("workspace rows chat_id=%s => %s", chat_id, payload)
        return payload
    except Exception:
        logging.exception("workspace rows inspect failed chat_id=%s", chat_id)
        return []

router = Router()

LIVE_MIRROR_TASKS = {}
LIVE_DP = None
PRIORITY_QUEUE_USERS = {
    713807432: "oveiro",
    626387429: "tyyttooo",
}

START_RENDER_CACHE: dict[int, tuple[float, str]] = {}
QUEUE_COUNTS_CACHE: tuple[float, dict[str, tuple[int, int]]] | None = None
JOIN_CHECK_CACHE: dict[int, tuple[float, bool]] = {}

def msk_now() -> datetime:
    return datetime.utcnow() + MSK_OFFSET

def now_str() -> str:
    return msk_now().strftime("%Y-%m-%d %H:%M:%S")


class SubmitStates(StatesGroup):
    waiting_mode = State()
    waiting_operator = State()
    waiting_qr = State()


class WithdrawStates(StatesGroup):
    waiting_amount = State()
    waiting_payment_link = State()

class MirrorStates(StatesGroup):
    waiting_token = State()

class EmojiLookupStates(StatesGroup):
    waiting_target = State()



class AdminStates(StatesGroup):
    waiting_hold = State()
    waiting_min_withdraw = State()
    waiting_treasury_add = State()
    waiting_treasury_sub = State()
    waiting_treasury_invoice = State()
    waiting_operator_price = State()
    waiting_group_finance_amount = State()
    waiting_group_price_value = State()
    waiting_group_default_price_value = State()
    waiting_role_user = State()
    waiting_role_kind = State()
    waiting_start_text = State()
    waiting_ad_text = State()
    waiting_broadcast_text = State()
    waiting_user_action_id = State()
    waiting_user_action_value = State()
    waiting_user_action_text = State()
    waiting_user_custom_price_text = State()
    waiting_user_stats_lookup = State()
    waiting_user_price_lookup = State()
    waiting_user_price_value = State()
    waiting_group_stats_lookup = State()
    waiting_db_upload = State()
    waiting_channel_value = State()
    waiting_backup_channel = State()
    waiting_required_join_link = State()
    waiting_required_join_item = State()
    waiting_required_join_remove = State()
    waiting_new_operator = State()
    waiting_new_operator_emoji = State()
    waiting_remove_operator = State()
    waiting_remove_group = State()
    waiting_summary_date = State()


@dataclass
class QueueItem:
    id: int
    user_id: int
    username: str
    full_name: str
    operator_key: str
    phone_label: str
    normalized_phone: str
    qr_file_id: str
    status: str
    price: float
    created_at: str
    taken_by_admin: Optional[int]
    taken_at: Optional[str]
    hold_until: Optional[str]
    work_started_at: Optional[str]
    mode: str
    started_notice_sent: int
    work_chat_id: Optional[int]
    work_thread_id: Optional[int]
    work_message_id: Optional[int]
    work_started_by: Optional[int]
    fail_reason: Optional[str]
    completed_at: Optional[str]
    timer_last_render: Optional[str]
    submit_bot_token: Optional[str] = None
    charge_chat_id: Optional[int] = None
    charge_thread_id: Optional[int] = None
    charge_amount: Optional[float] = None
    user_hold_chat_id: Optional[int] = None
    user_hold_message_id: Optional[int] = None
    qr_blob: Optional[bytes] = None
    qr_mime: Optional[str] = None
    qr_filename: Optional[str] = None

    @classmethod
    def from_row(cls, row):
        if row is None:
            return None
        data = dict(row)
        allowed = cls.__annotations__.keys()
        return cls(**{k: data.get(k) for k in allowed})


class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        self.seed_defaults()

    def reconnect(self):
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row

    def replace_with_uploaded_db(self, uploaded_path: str):
        temp_uploaded = Path(uploaded_path)
        backup_path = Path(self.path + '.backup')
        current_path = Path(self.path)
        if current_path.exists():
            try:
                self.conn.commit()
            except Exception:
                pass
            try:
                shutil.copyfile(current_path, backup_path)
            except Exception:
                logging.exception("failed to create DB backup before replace")
        try:
            self.conn.execute("ATTACH DATABASE ? AS uploaded", (str(temp_uploaded),))
            self.conn.execute("PRAGMA foreign_keys=OFF")
            tables_to_copy = [
                "users",
                "queue_items",
                "withdrawals",
                "mirrors",
                "payout_accounts",
                "workspaces",
                "treasury_invoices",
                "group_finance",
                "settings",
                "custom_operators",
                "group_operator_prices",
                "user_prices",
                "roles",
            ]
            for table in tables_to_copy:
                try:
                    main_cols = [r[1] for r in self.conn.execute(f"PRAGMA main.table_info({table})").fetchall()]
                    up_cols = [r[1] for r in self.conn.execute(f"PRAGMA uploaded.table_info({table})").fetchall()]
                    common = [c for c in main_cols if c in up_cols]
                    if not common:
                        continue
                    cols_sql = ",".join(common)
                    self.conn.execute(f"DELETE FROM main.{table}")
                    self.conn.execute(f"INSERT INTO main.{table} ({cols_sql}) SELECT {cols_sql} FROM uploaded.{table}")
                except Exception:
                    logging.exception("copy table failed: %s", table)
            self.conn.commit()
        finally:
            try:
                self.conn.execute("DETACH DATABASE uploaded")
            except Exception:
                pass
            try:
                temp_uploaded.unlink(missing_ok=True)
            except Exception:
                pass

        self.create_tables()
        self.seed_defaults()
        try:
            ensure_extra_schema()
        except Exception:
            pass
        try:
            globals().get('restore_operators_from_db_anywhere', lambda: None)()
        except Exception:
            logging.exception('post-upload operator restore failed')
        try:
            cleanup_database_size(18)
        except Exception:
            logging.exception('post-upload cleanup failed')
        return backup_path

    def create_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                balance REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS roles (
                user_id INTEGER PRIMARY KEY,
                role TEXT NOT NULL,
                assigned_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                mode TEXT NOT NULL,
                added_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                chat_title TEXT,
                thread_title TEXT,
                UNIQUE(chat_id, thread_id, mode)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS queue_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                operator_key TEXT NOT NULL,
                phone_label TEXT NOT NULL,
                normalized_phone TEXT NOT NULL,
                qr_file_id TEXT NOT NULL,
                qr_blob BLOB,
                qr_mime TEXT,
                qr_filename TEXT,
                status TEXT NOT NULL,
                price REAL NOT NULL,
                created_at TEXT NOT NULL,
                taken_by_admin INTEGER,
                taken_at TEXT,
                hold_until TEXT,
                work_started_at TEXT,
                mode TEXT NOT NULL DEFAULT 'hold',
                started_notice_sent INTEGER DEFAULT 0,
                work_chat_id INTEGER,
                work_thread_id INTEGER,
                work_message_id INTEGER,
                work_started_by INTEGER,
                fail_reason TEXT,
                completed_at TEXT,
                timer_last_render TEXT,
                submit_bot_token TEXT,
                charge_chat_id INTEGER,
                charge_thread_id INTEGER,
                charge_amount REAL,
                user_hold_chat_id INTEGER,
                user_hold_message_id INTEGER,
                charge_refunded INTEGER NOT NULL DEFAULT 0,
                operator_title_snapshot TEXT,
                operator_command_snapshot TEXT,
                operator_emoji_id_snapshot TEXT,
                operator_emoji_snapshot TEXT
            )
            """
        )


        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_prices (
                user_id INTEGER NOT NULL,
                operator_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, operator_key, mode)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS payout_accounts (
                user_id INTEGER PRIMARY KEY,
                payout_link TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                decided_at TEXT,
                admin_id INTEGER,
                payout_check TEXT,
                payout_note TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mirrors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_user_id INTEGER NOT NULL,
                owner_username TEXT,
                token TEXT NOT NULL UNIQUE,
                bot_id INTEGER,
                bot_username TEXT,
                bot_title TEXT,
                status TEXT NOT NULL DEFAULT 'saved',
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS treasury_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount REAL NOT NULL,
                crypto_invoice_id TEXT,
                pay_url TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                paid_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS group_finance (
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                treasury_balance REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, thread_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS group_operator_prices (
                chat_id INTEGER NOT NULL,
                thread_id INTEGER,
                operator_key TEXT NOT NULL,
                mode TEXT NOT NULL,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, thread_id, operator_key, mode)
            )
            """
        )
        self.conn.commit()

    def seed_defaults(self):
        defaults = {
            "hold_minutes": str(DEFAULT_HOLD_MINUTES),
            "min_withdraw": str(MIN_WITHDRAW),
            "treasury_balance": str(DEFAULT_TREASURY_BALANCE),
            "start_title": "ESIM Service X",
            "start_subtitle": "Премиум сервис приёма номеров",
            "start_description": "🚀 <b>Быстрый приём заявок</b> • 💎 <b>Стабильные выплаты</b> • 🛡 <b>Контроль статусов</b>",
            "announcement_text": "",
            "withdraw_channel_id": str(WITHDRAW_CHANNEL_ID),
            "withdraw_thread_id": "0",
            "log_channel_id": str(LOG_CHANNEL_ID),
            "backup_channel_id": "0",
            "backup_enabled": "0",
            "required_join_chat_id": "0",
            "required_join_link": "",
        }
        for key, value in defaults.items():
            self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
        for key, data in OPERATORS.items():
            hold_price = float(PERMANENT_OPERATOR_PRICES.get(key, {}).get("hold", data["price"]))
            no_hold_price = float(PERMANENT_OPERATOR_PRICES.get(key, {}).get("no_hold", data["price"]))
            self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (f"price_{key}", str(hold_price)))
            self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (f"price_hold_{key}", str(hold_price)))
            self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (f"price_no_hold_{key}", str(no_hold_price)))
            self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (f"allow_hold_{key}", "1"))
            self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (f"allow_no_hold_{key}", "1"))
        self.conn.execute(
            "INSERT OR IGNORE INTO roles (user_id, role, assigned_at) VALUES (?, 'chief_admin', ?)",
            (CHIEF_ADMIN_ID, now_str()),
        )
        for uid in BOOTSTRAP_ADMINS:
            if uid != CHIEF_ADMIN_ID:
                self.conn.execute(
                    "INSERT OR IGNORE INTO roles (user_id, role, assigned_at) VALUES (?, 'admin', ?)",
                    (uid, now_str()),
                )
        for uid in BOOTSTRAP_OPERATORS:
            self.conn.execute(
                "INSERT OR IGNORE INTO roles (user_id, role, assigned_at) VALUES (?, 'operator', ?)",
                (uid, now_str()),
            )
        self.conn.commit()


    def save_mirror(self, owner_user_id: int, owner_username: str, token: str, bot_id: int, bot_username: str, bot_title: str):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO mirrors (owner_user_id, owner_username, token, bot_id, bot_username, bot_title, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'active', ?)
            ON CONFLICT(token) DO UPDATE SET
                owner_user_id=excluded.owner_user_id,
                owner_username=excluded.owner_username,
                bot_id=excluded.bot_id,
                bot_username=excluded.bot_username,
                bot_title=excluded.bot_title,
                status='active'
            """,
            (owner_user_id, owner_username, token, bot_id, bot_username, bot_title, now_str()),
        )
        self.conn.commit()
        return cur.lastrowid

    def user_mirrors(self, owner_user_id: int):
        return self.conn.execute(
            "SELECT * FROM mirrors WHERE owner_user_id=? ORDER BY id DESC LIMIT 10",
            (owner_user_id,),
        ).fetchall()

    def all_active_mirrors(self):
        return self.conn.execute(
            "SELECT * FROM mirrors WHERE status IN ('saved','active') ORDER BY id ASC"
        ).fetchall()

    def get_setting(self, key: str, default: Optional[str] = None) -> str:
        row = self.conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        self.conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def upsert_user(self, user_id: int, username: str, full_name: str):
        self.conn.execute(
            """
            INSERT INTO users (user_id, username, full_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name
            """,
            (user_id, username, full_name),
        )
        self.conn.commit()


    def find_user_by_username(self, username: str):
        username = (username or "").lstrip("@").strip().lower()
        if not username:
            return None
        return self.conn.execute("SELECT * FROM users WHERE lower(username)=?", (username,)).fetchone()

    def find_last_user_by_phone(self, phone: str):
        normalized = normalize_phone(phone) if phone else None
        if not normalized:
            return None
        return self.conn.execute(
            "SELECT u.* FROM queue_items q JOIN users u ON u.user_id=q.user_id WHERE q.normalized_phone=? ORDER BY q.id DESC LIMIT 1",
            (normalized,),
        ).fetchone()

    def all_user_ids(self):
        rows = self.conn.execute("SELECT user_id FROM users ORDER BY user_id ASC").fetchall()
        return [int(r["user_id"]) for r in rows]

    def export_usernames(self) -> str:
        rows = self.conn.execute("SELECT username FROM users WHERE username IS NOT NULL AND username != '' ORDER BY username COLLATE NOCASE").fetchall()
        return "\n".join(f"@{r['username'].lstrip('@')}" for r in rows)

    def get_user(self, user_id: int):
        return self.conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()

    def add_balance(self, user_id: int, amount: float):
        self.conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
        self.conn.commit()

    def subtract_balance(self, user_id: int, amount: float):
        self.conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
        self.conn.commit()

    def set_role(self, user_id: int, role: str):
        current = self.get_role(user_id)
        if current == "chief_admin" and role != "chief_admin":
            return False
        self.conn.execute(
            "INSERT INTO roles (user_id, role, assigned_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, assigned_at=excluded.assigned_at",
            (user_id, role, now_str()),
        )
        self.conn.commit()
        return True

    def remove_role(self, user_id: int):
        if user_id == CHIEF_ADMIN_ID:
            return False
        self.conn.execute("DELETE FROM roles WHERE user_id = ?", (user_id,))
        self.conn.commit()
        return True

    def get_role(self, user_id: int) -> str:
        if user_id == CHIEF_ADMIN_ID:
            return "chief_admin"
        row = self.conn.execute("SELECT role FROM roles WHERE user_id = ?", (user_id,)).fetchone()
        return row["role"] if row else "user"

    def list_roles(self):
        return self.conn.execute("SELECT * FROM roles ORDER BY CASE role WHEN 'chief_admin' THEN 0 WHEN 'admin' THEN 1 WHEN 'operator' THEN 2 ELSE 3 END, user_id ASC").fetchall()

    def get_operator_price(self, operator_key: str) -> float:
        return float(self.get_setting(f"price_{operator_key}", str(OPERATORS[operator_key]["price"])))

    def create_queue_item(self, user_id: int, username: str, full_name: str, operator_key: str, normalized_phone: str, qr_file_id: str, mode: str):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO queue_items (
                user_id, username, full_name, operator_key, phone_label, normalized_phone,
                qr_file_id, status, price, created_at, mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)
            """,
            (
                user_id,
                username,
                full_name,
                operator_key,
                pretty_phone(normalized_phone),
                normalized_phone,
                qr_file_id,
                get_mode_price(operator_key, mode, user_id),
                now_str(),
                mode,
            ),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_queue_item(self, item_id: int):
        row = self.conn.execute("SELECT * FROM queue_items WHERE id = ?", (item_id,)).fetchone()
        return QueueItem.from_row(row)

    def get_next_queue_item(self, operator_key: str):
        row = self.conn.execute(
            "SELECT * FROM queue_items WHERE operator_key = ? AND status = 'queued' ORDER BY " + queue_order_sql() + " LIMIT 1",
            (operator_key,),
        ).fetchone()
        return QueueItem.from_row(row)

    def count_waiting(self, operator_key: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS c FROM queue_items WHERE operator_key=? AND status='queued'",
            (operator_key,),
        ).fetchone()
        return int(row["c"] or 0)

    def mark_taken(self, item_id: int, user_id: int):
        self.conn.execute(
            "UPDATE queue_items SET status='taken', taken_by_admin=?, taken_at=? WHERE id=? AND status='queued'",
            (user_id, now_str(), item_id),
        )
        self.conn.commit()

    def mark_error_before_start(self, item_id: int):
        self.conn.execute(
            "UPDATE queue_items SET status='failed', fail_reason='error_before_start', completed_at=? WHERE id=?",
            (now_str(), item_id),
        )
        self.conn.commit()

    def start_work(self, item_id: int, worker_id: int, mode: str, chat_id: int, thread_id: Optional[int], message_id: int):
        start_dt = msk_now()
        hold_until = None
        if mode == "hold":
            hold_minutes = int(float(self.get_setting("hold_minutes", str(DEFAULT_HOLD_MINUTES))))
            hold_until = fmt_dt(start_dt + timedelta(minutes=hold_minutes))
        self.conn.execute(
            """
            UPDATE queue_items
            SET status='in_progress', work_started_at=?, hold_until=?, started_notice_sent=1,
                work_chat_id=?, work_thread_id=?, work_message_id=?, work_started_by=?, timer_last_render=?
            WHERE id=?
            """,
            (fmt_dt(start_dt), hold_until, chat_id, thread_id, message_id, worker_id, fmt_dt(start_dt), item_id),
        )
        self.conn.commit()

    def fail_after_start(self, item_id: int, reason: str):
        self.conn.execute(
            "UPDATE queue_items SET status='failed', fail_reason=?, completed_at=? WHERE id=?",
            (reason, now_str(), item_id),
        )
        self.conn.commit()

    def complete_queue_item(self, item_id: int):
        self.conn.execute(
            "UPDATE queue_items SET status='completed', completed_at=? WHERE id=?",
            (now_str(), item_id),
        )
        self.conn.commit()

    def get_expired_holds(self):
        rows = self.conn.execute(
            "SELECT * FROM queue_items WHERE status='in_progress' AND mode='hold' AND hold_until IS NOT NULL AND hold_until <= ?",
            (now_str(),),
        ).fetchall()
        return [QueueItem.from_row(row) for row in rows]

    def get_active_holds_for_render(self):
        rows = self.conn.execute(
            "SELECT * FROM queue_items WHERE status='in_progress' AND mode='hold' AND hold_until IS NOT NULL AND work_chat_id IS NOT NULL AND work_message_id IS NOT NULL"
        ).fetchall()
        return [QueueItem.from_row(row) for row in rows]

    def touch_timer_render(self, item_id: int):
        self.conn.execute("UPDATE queue_items SET timer_last_render=? WHERE id=?", (now_str(), item_id))
        self.conn.commit()



    def set_user_price(self, user_id: int, operator_key: str, mode: str, price: float):
        self.conn.execute(
            "INSERT INTO user_prices (user_id, operator_key, mode, price, updated_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id, operator_key, mode) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at",
            (user_id, operator_key, mode, price, now_str()),
        )
        self.conn.commit()

    def delete_user_price(self, user_id: int, operator_key: str, mode: str):
        self.conn.execute(
            "DELETE FROM user_prices WHERE user_id=? AND operator_key=? AND mode=?",
            (user_id, operator_key, mode),
        )
        self.conn.commit()

    def get_user_price(self, user_id: int, operator_key: str, mode: str):
        row = self.conn.execute(
            "SELECT price FROM user_prices WHERE user_id=? AND operator_key=? AND mode=?",
            (user_id, operator_key, mode),
        ).fetchone()
        return float(row["price"]) if row else None

    def list_user_prices(self, user_id: int):
        return self.conn.execute(
            "SELECT * FROM user_prices WHERE user_id=? ORDER BY operator_key, mode",
            (user_id,),
        ).fetchall()

    def set_payout_link(self, user_id: int, payout_link: str):
        self.conn.execute(
            "INSERT INTO payout_accounts (user_id, payout_link, updated_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET payout_link=excluded.payout_link, updated_at=excluded.updated_at",
            (user_id, payout_link, now_str()),
        )
        self.conn.commit()

    def get_payout_link(self, user_id: int) -> Optional[str]:
        row = self.conn.execute("SELECT payout_link FROM payout_accounts WHERE user_id=?", (user_id,)).fetchone()
        return row["payout_link"] if row else None

    def create_withdrawal(self, user_id: int, amount: float):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO withdrawals (user_id, amount, status, created_at) VALUES (?, ?, 'pending', ?)",
            (user_id, amount, now_str()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_withdrawal(self, withdraw_id: int):
        return self.conn.execute("SELECT * FROM withdrawals WHERE id = ?", (withdraw_id,)).fetchone()

    def set_withdrawal_status(self, withdraw_id: int, status: str, admin_id: int, payout_check: Optional[str] = None, payout_note: Optional[str] = None):
        self.conn.execute(
            "UPDATE withdrawals SET status=?, decided_at=?, admin_id=?, payout_check=?, payout_note=? WHERE id=?",
            (status, now_str(), admin_id, payout_check, payout_note, withdraw_id),
        )
        self.conn.commit()

    def count_pending_withdrawals(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM withdrawals WHERE status='pending'").fetchone()
        return int(row["c"] or 0)


    def create_treasury_invoice(self, amount: float, crypto_invoice_id: Optional[str], pay_url: Optional[str], created_by: int):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO treasury_invoices (amount, crypto_invoice_id, pay_url, status, created_by, created_at) VALUES (?, ?, ?, 'active', ?, ?)",
            (amount, str(crypto_invoice_id or ''), pay_url or '', created_by, now_str()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_treasury_invoice(self, invoice_id: int):
        return self.conn.execute("SELECT * FROM treasury_invoices WHERE id = ?", (invoice_id,)).fetchone()

    def mark_treasury_invoice_paid(self, invoice_id: int):
        self.conn.execute("UPDATE treasury_invoices SET status='paid', paid_at=? WHERE id=?", (now_str(), invoice_id))
        self.conn.commit()

    def list_recent_treasury_invoices(self, limit: int = 10):
        return self.conn.execute("SELECT * FROM treasury_invoices ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    def get_treasury(self) -> float:
        return float(self.get_setting("treasury_balance", str(DEFAULT_TREASURY_BALANCE)))

    def add_treasury(self, amount: float):
        self.set_setting("treasury_balance", str(self.get_treasury() + amount))

    def subtract_treasury(self, amount: float):
        self.set_setting("treasury_balance", str(self.get_treasury() - amount))

    def _thread_key(self, thread_id: Optional[int]):
        return -1 if thread_id is None else int(thread_id)

    def get_group_balance(self, chat_id: int, thread_id: Optional[int]) -> float:
        row = self.conn.execute(
            "SELECT treasury_balance FROM group_finance WHERE chat_id=? AND thread_id=?",
            (int(chat_id), self._thread_key(thread_id)),
        ).fetchone()
        return float(row["treasury_balance"]) if row else 0.0

    def set_group_balance(self, chat_id: int, thread_id: Optional[int], balance: float):
        self.conn.execute(
            "INSERT INTO group_finance (chat_id, thread_id, treasury_balance, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(chat_id, thread_id) DO UPDATE SET treasury_balance=excluded.treasury_balance, updated_at=excluded.updated_at",
            (int(chat_id), self._thread_key(thread_id), float(balance), now_str()),
        )
        self.conn.commit()

    def add_group_balance(self, chat_id: int, thread_id: Optional[int], amount: float):
        self.set_group_balance(chat_id, thread_id, self.get_group_balance(chat_id, thread_id) + float(amount))

    def subtract_group_balance(self, chat_id: int, thread_id: Optional[int], amount: float):
        self.set_group_balance(chat_id, thread_id, self.get_group_balance(chat_id, thread_id) - float(amount))

    def get_group_price(self, chat_id: int, thread_id: Optional[int], operator_key: str, mode: str):
        row = self.conn.execute(
            "SELECT price FROM group_operator_prices WHERE chat_id=? AND thread_id=? AND operator_key=? AND mode=?",
            (int(chat_id), self._thread_key(thread_id), operator_key, mode),
        ).fetchone()
        return float(row["price"]) if row else None

    def set_group_price(self, chat_id: int, thread_id: Optional[int], operator_key: str, mode: str, price: float):
        self.conn.execute(
            "INSERT INTO group_operator_prices (chat_id, thread_id, operator_key, mode, price, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(chat_id, thread_id, operator_key, mode) DO UPDATE SET price=excluded.price, updated_at=excluded.updated_at",
            (int(chat_id), self._thread_key(thread_id), operator_key, mode, float(price), now_str()),
        )
        self.conn.commit()

    def reserve_queue_item_for_group(self, item_id: int, taker_id: int, chat_id: int, thread_id: Optional[int], amount: float) -> bool:
        current_balance = self.get_group_balance(chat_id, thread_id)
        if current_balance + 1e-9 < float(amount):
            return False
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE queue_items SET status='taken', taken_by_admin=?, taken_at=?, charge_chat_id=?, charge_thread_id=?, charge_amount=?, charge_refunded=0 WHERE id=? AND status='queued'",
            (taker_id, now_str(), int(chat_id), self._thread_key(thread_id), float(amount), item_id),
        )
        if cur.rowcount <= 0:
            self.conn.rollback()
            return False
        self.conn.execute(
            "INSERT INTO group_finance (chat_id, thread_id, treasury_balance, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(chat_id, thread_id) DO UPDATE SET treasury_balance=excluded.treasury_balance, updated_at=excluded.updated_at",
            (int(chat_id), self._thread_key(thread_id), current_balance - float(amount), now_str()),
        )
        self.conn.commit()
        return True

    def release_item_reservation(self, item_id: int) -> float:
        row = self.conn.execute("SELECT charge_chat_id, charge_thread_id, charge_amount, COALESCE(charge_refunded, 0) AS charge_refunded FROM queue_items WHERE id=?", (item_id,)).fetchone()
        if not row or row["charge_chat_id"] is None or row["charge_amount"] is None:
            return 0.0
        if int(row["charge_refunded"] or 0) == 1:
            return 0.0
        amount = float(row["charge_amount"] or 0)
        thread_id = None if int(row["charge_thread_id"]) == -1 else int(row["charge_thread_id"])
        self.add_group_balance(int(row["charge_chat_id"]), thread_id, amount)
        self.conn.execute("UPDATE queue_items SET charge_refunded=1 WHERE id=?", (item_id,))
        self.conn.commit()
        return amount

    def enable_workspace(self, chat_id: int, thread_id: Optional[int], mode: str, added_by: int):
        thread_key = self._thread_key(thread_id)
        row = self.conn.execute("SELECT id FROM workspaces WHERE chat_id=? AND thread_id=? AND mode=? ORDER BY id DESC LIMIT 1", (chat_id, thread_key, mode)).fetchone()
        if row:
            self.conn.execute("UPDATE workspaces SET is_enabled=1, added_by=?, created_at=? WHERE id=?", (added_by, now_str(), int(row['id'])))
            self.conn.execute("DELETE FROM workspaces WHERE chat_id=? AND thread_id=? AND mode=? AND id<>?", (chat_id, thread_key, mode, int(row['id'])))
        else:
            self.conn.execute(
                "INSERT INTO workspaces (chat_id, thread_id, mode, added_by, created_at, is_enabled) VALUES (?, ?, ?, ?, ?, 1)",
                (chat_id, thread_key, mode, added_by, now_str()),
            )
        self.conn.commit()

    def disable_workspace(self, chat_id: int, thread_id: Optional[int], mode: str):
        thread_key = self._thread_key(thread_id)
        self.conn.execute(
            "UPDATE workspaces SET is_enabled=0 WHERE chat_id=? AND thread_id=? AND mode=?",
            (chat_id, thread_key, mode),
        )
        self.conn.commit()

    def is_workspace_enabled(self, chat_id: int, thread_id: Optional[int], mode: str) -> bool:
        thread_key = self._thread_key(thread_id)
        row = self.conn.execute(
            "SELECT is_enabled FROM workspaces WHERE chat_id=? AND thread_id=? AND mode=? ORDER BY id DESC LIMIT 1",
            (chat_id, thread_key, mode),
        ).fetchone()
        return bool(row and row["is_enabled"])

    def list_workspaces(self):
        return self.conn.execute("SELECT * FROM workspaces WHERE is_enabled=1 ORDER BY chat_id, thread_id").fetchall()

    def user_stats(self, user_id: int):
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN status='taken' THEN 1 ELSE 0 END) AS taken,
                SUM(CASE WHEN status='in_progress' THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slipped,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS earned
            FROM queue_items WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()
        return row

    def user_operator_stats(self, user_id: int):
        return self.conn.execute(
            "SELECT operator_key, COUNT(*) AS total, SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS earned FROM queue_items WHERE user_id=? GROUP BY operator_key ORDER BY total DESC",
            (user_id,),
        ).fetchall()


    def recover_after_restart(self):
        # Return items that were merely taken but never started back into the queue
        self.conn.execute(
            """
            UPDATE queue_items
            SET status='queued',
                taken_by_admin=NULL,
                taken_at=NULL
            WHERE status='taken' AND (work_started_at IS NULL OR work_started_at='')
            """
        )
        # Force timer re-render on active holds after restart
        self.conn.execute(
            "UPDATE queue_items SET timer_last_render=NULL WHERE status='in_progress' AND mode='hold'"
        )
        self.conn.commit()

    def group_stats(self, chat_id: int, thread_id: Optional[int]):
        return self.conn.execute(
            """
            SELECT
                COUNT(*) AS taken_total,
                SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS spent_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
            FROM queue_items
            WHERE charge_chat_id=? AND charge_thread_id=?
            """,
            (int(chat_id), self._thread_key(thread_id)),
        ).fetchone()


db = Database(DB_PATH)


def msk_now() -> datetime:
    return datetime.utcnow() + MSK_OFFSET

def now_str() -> str:
    return msk_now().strftime("%Y-%m-%d %H:%M:%S")

def msk_today_bounds_str() -> tuple[str, str, str]:
    now = msk_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    label = start.strftime("%d.%m.%Y")
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), label

def msk_stats_reset_note() -> str:
    return "Сброс каждый день в 00:00 МСК"



def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def usd(amount: float) -> str:
    return f"${float(amount or 0):.2f}"


def user_role(user_id: int) -> str:
    return db.get_role(user_id)


def is_admin(user_id: int) -> bool:
    return user_role(user_id) in {"chief_admin", "admin"}


def is_operator_or_admin(user_id: int) -> bool:
    return user_role(user_id) in {"chief_admin", "admin", "operator"}



async def callback_actor_can_take_esim(callback: CallbackQuery) -> tuple[bool, str]:
    msg = getattr(callback, "message", None)
    user = getattr(callback, "from_user", None)
    if not msg or not user:
        return False, "no_message_or_user"
    role = user_role(user.id)
    if role in {"chief_admin", "admin", "operator"}:
        return True, f"internal_role:{role}"
    if msg.chat.type == ChatType.PRIVATE:
        return False, f"internal_role:{role or 'none'}"
    try:
        member = await callback.bot.get_chat_member(msg.chat.id, user.id)
        status = str(getattr(member, "status", "unknown"))
        if status in {"creator", "administrator", "member", "restricted"}:
            return True, f"chat_member:{status}"
        return False, f"chat_member:{status}"
    except Exception:
        logging.exception("callback_actor_can_take_esim get_chat_member failed chat_id=%s user_id=%s; allow until workspace check", msg.chat.id, user.id)
        return True, "group_user_check_failed"


async def message_actor_can_take_esim(message: Message) -> tuple[bool, str]:
    """Who may open /esim.

    In work groups /esim must be available to regular group members too,
    because the real protection is the /work or /topic workspace check below.
    Private chats stay blocked.
    """
    user = getattr(message, "from_user", None)
    if not user:
        return False, "no_user"
    role = user_role(user.id)
    if role in {"chief_admin", "admin", "operator"}:
        return True, f"internal_role:{role}"
    if message.chat.type == ChatType.PRIVATE:
        return False, f"internal_role:{role or 'none'}"
    try:
        member = await message.bot.get_chat_member(message.chat.id, user.id)
        status = str(getattr(member, "status", "unknown"))
        if status in {"creator", "administrator"}:
            return True, f"chat_admin:{status}"
        if status in {"member", "restricted"}:
            return True, f"chat_member:{status}"
        return False, f"chat_member:{status}"
    except Exception:
        # If Telegram member check fails, still allow in non-private chat.
        # Workspace check after this will block non-working chats.
        logging.exception("message_actor_can_take_esim get_chat_member failed chat_id=%s user_id=%s; allow until workspace check", message.chat.id, user.id)
        return True, f"group_user_check_failed"


def is_chief_admin(user_id: int) -> bool:
    return user_role(user_id) == "chief_admin"

def is_backup_enabled() -> bool:
    return db.get_setting("backup_enabled", "0") == "1"

def set_backup_enabled(enabled: bool):
    db.set_setting("backup_enabled", "1" if enabled else "0")

def backup_channel_id() -> int:
    try:
        return int(db.get_setting("backup_channel_id", "0") or 0)
    except Exception:
        return 0

def log_channel_id() -> int:
    try:
        return int(db.get_setting("log_channel_id", str(LOG_CHANNEL_ID)) or 0)
    except Exception:
        return 0



def normalize_phone(raw: str) -> Optional[str]:
    text = (raw or "").strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if text.startswith("+"):
        text = text[1:]
    if len(text) == 11 and text.isdigit() and text[0] in {"7", "8"}:
        return "7" + text[1:]
    return None


def pretty_phone(normalized: str) -> str:
    return f"+{normalized}" if normalized else "-"


def progress_bar(hold_until: Optional[str], started_at: Optional[str], size: int = 10) -> str:
    start = parse_dt(started_at)
    end = parse_dt(hold_until)
    if not start or not end:
        return ""
    total = max((end - start).total_seconds(), 1)
    left = max((end - msk_now()).total_seconds(), 0)
    done = max(total - left, 0)
    filled = min(size, max(0, round(done / total * size)))
    return "🟩" * filled + "⬜" * (size - filled)


def time_left_text(hold_until: Optional[str]) -> str:
    end = parse_dt(hold_until)
    if not end:
        return "—"
    left = end - msk_now()
    if left.total_seconds() <= 0:
        return "00:00"
    total = int(left.total_seconds())
    minutes = total // 60
    seconds = total % 60
    return f"{minutes:02d}:{seconds:02d}"


def required_join_entries() -> list[dict]:
    raw = (db.get_setting("required_join_items", "") or "").strip()
    items = []
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for item in parsed:
                    if not isinstance(item, dict):
                        continue
                    chat_id = item.get("chat_id")
                    link = (item.get("link") or "").strip()
                    title = (item.get("title") or "").strip()
                    try:
                        chat_id = int(chat_id)
                    except Exception:
                        continue
                    if chat_id:
                        items.append({"chat_id": chat_id, "link": link, "title": title})
        except Exception:
            logging.exception("failed to parse required_join_items")
    if items:
        return items

    # backward compatibility with old single-group settings
    try:
        legacy_chat_id = int(db.get_setting("required_join_chat_id", "0") or 0)
    except Exception:
        legacy_chat_id = 0
    legacy_link = (db.get_setting("required_join_link", "") or "").strip()
    if legacy_chat_id:
        return [{"chat_id": legacy_chat_id, "link": legacy_link, "title": ""}]
    return []

def save_required_join_entries(items: list[dict]):
    normalized = []
    seen = set()
    for item in items:
        try:
            chat_id = int(item.get("chat_id"))
        except Exception:
            continue
        if not chat_id or chat_id in seen:
            continue
        seen.add(chat_id)
        normalized.append({
            "chat_id": chat_id,
            "link": (item.get("link") or "").strip(),
            "title": (item.get("title") or "").strip(),
        })
    db.set_setting("required_join_items", json.dumps(normalized, ensure_ascii=False))
    # keep legacy fields in sync with the first item
    if normalized:
        db.set_setting("required_join_chat_id", str(normalized[0]["chat_id"]))
        db.set_setting("required_join_link", normalized[0]["link"])
    else:
        db.set_setting("required_join_chat_id", "0")
        db.set_setting("required_join_link", "")

def render_required_join_admin() -> str:
    items = required_join_entries()
    lines = ["<b>👥 Обязательная подписка</b>", ""]
    if not items:
        lines.append("Сейчас обязательная подписка <b>выключена</b>.")
    else:
        lines.append(f"Подписок в списке: <b>{len(items)}</b>")
        lines.append("")
        for idx, item in enumerate(items, 1):
            title = escape(item.get("title") or f"Канал {idx}")
            lines.append(f"<b>{idx}.</b> {title}")
            lines.append(f"ID: <code>{item['chat_id']}</code>")
            if item.get("link"):
                lines.append(f"Ссылка: <code>{escape(item['link'])}</code>")
            lines.append("")
    lines.append("Формат добавления: <code>-100xxxxxxxxxx | https://t.me/your_link | Название</code>")
    lines.append("Название можно не указывать.")
    return "\n".join(lines).strip()

def required_join_chat_id() -> int:
    items = required_join_entries()
    return int(items[0]["chat_id"]) if items else 0

def required_join_link() -> str:
    items = required_join_entries()
    return (items[0].get("link") or "").strip() if items else ""

def subscription_required_enabled() -> bool:
    return bool(required_join_entries())

def required_join_check_bot(current_bot: Bot | None = None) -> Bot | None:
    primary = PRIMARY_BOT
    if primary is not None:
        return primary
    return current_bot

async def is_user_joined_required_group(bot: Bot, user_id: int) -> bool:
    items = required_join_entries()
    if not items:
        return True
    check_bot = required_join_check_bot(bot)
    if check_bot is None:
        return False
    for item in items:
        try:
            member = await check_bot.get_chat_member(int(item["chat_id"]), user_id)
            if getattr(member, 'status', '') not in {'creator', 'administrator', 'member', 'restricted'}:
                return False
        except Exception:
            logging.exception(
                'required group membership check failed for user_id=%s chat_id=%s via_bot=%s',
                user_id,
                item.get("chat_id"),
                getattr(check_bot, 'token', '')[:12] + '...' if getattr(check_bot, 'token', None) else 'unknown',
            )
            return False
    return True

def required_join_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for item in required_join_entries()[:10]:
        link = (item.get("link") or "").strip()
        if not link:
            continue
        title = (item.get("title") or "").strip() or f"Канал {str(item['chat_id'])[-4:]}"
        kb.row(InlineKeyboardButton(text=f'👥 {title}', url=link))
    kb.button(text='✅ Проверить подписку', callback_data='join:check')
    kb.adjust(1)
    return kb

async def ensure_required_subscription_entity(entity, bot: Bot, user_id: int) -> bool:
    if not subscription_required_enabled():
        return True
    joined = await is_user_joined_required_group(bot, user_id)
    if joined:
        return True
    text = (
        '<b>🔒 Доступ ограничен</b>\n\n'
        'Для использования бота нужна обязательная подписка на группу.\n\n'
        'После вступления нажмите <b>«Проверить подписку»</b>.'
    )
    await send_banner_message(entity, db.get_setting('start_banner_path', START_BANNER), text, required_join_kb().as_markup())
    return False

def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Сдать номер", callback_data="menu:submit")
    kb.button(text="📦 Мои номера", callback_data="menu:my")
    kb.button(text="👤 Профиль", callback_data="menu:profile")
    kb.button(text="🎁 Реф. система", callback_data="menu:ref")
    kb.button(text="💸 Вывод средств", callback_data="menu:withdraw")
    kb.button(text="🪞 Зеркало", callback_data="menu:mirror")
    kb.adjust(2)
    return kb.as_markup()


def profile_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 Мои номера", callback_data="menu:my")
    kb.button(text="🎁 Реф. система", callback_data="menu:ref")
    kb.button(text="💳 Изменить счёт", callback_data="menu:payout_link")
    kb.button(text="💸 Вывод средств", callback_data="menu:withdraw")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(2)
    return kb.as_markup()

MY_NUMBERS_PAGE_SIZE = 8

def my_numbers_kb(items, page: int = 0):
    kb = InlineKeyboardBuilder()
    total = len(items or [])
    max_page = max((total - 1) // MY_NUMBERS_PAGE_SIZE, 0)
    page = max(0, min(int(page or 0), max_page))
    start = page * MY_NUMBERS_PAGE_SIZE
    for item in (items or [])[start:start + MY_NUMBERS_PAGE_SIZE]:
        if item['status'] == 'queued':
            kb.button(text=f"🗑 Убрать #{item['id']}", callback_data=f"myremove:{item['id']}:{page}")
    if total > MY_NUMBERS_PAGE_SIZE:
        prev_page = max(page - 1, 0)
        next_page = min(page + 1, max_page)
        kb.button(text="⬅️ Назад", callback_data=f"my:page:{prev_page}")
        kb.button(text=f"{page + 1}/{max_page + 1}", callback_data="noop")
        kb.button(text="➡️ Далее", callback_data=f"my:page:{next_page}")
    kb.button(text="↻ Обновить", callback_data=f"my:page:{page}")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1, 3, 1, 1)
    return kb.as_markup()



def quick_submit_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Сдать ещё номер", callback_data="menu:submit")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(2)
    return kb.as_markup()

def mirror_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Создать зеркало", callback_data="mirror:create")
    kb.button(text="📋 Мои зеркала", callback_data="mirror:list")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(2)
    return kb.as_markup()
def cancel_inline_kb(back: str = "menu:home"):
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data=back)
    kb.adjust(1)
    return kb.as_markup()


def operators_kb(mode: str = "hold", prefix: str = "op", back_cb: str = "mode:back", user_id: int | None = None):
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        q = count_waiting_mode(key, mode)
        price = get_mode_price(key, mode, user_id)
        prefix_mark = "🚫 " if not is_operator_mode_enabled(key, mode) else ""
        kb.row(make_operator_button(operator_key=key, callback_data=f"{prefix}:{key}:{mode}", prefix_mark=prefix_mark, suffix_text=f" ({q}) • {usd(price)}"))
    kb.button(text="↩️ Назад", callback_data=back_cb)
    kb.adjust(1)
    return kb.as_markup()


def operators_group_kb(chat_id: int, thread_id: int | None, mode: str = "hold", prefix: str = "esim_take", back_cb: str = "esim:back_mode"):
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        q = count_waiting_mode(key, mode)
        price = group_price_for_take(chat_id, thread_id, key, mode)
        prefix_mark = "🚫 " if not is_operator_mode_enabled(key, mode) else ""
        kb.row(make_operator_button(operator_key=key, callback_data=f"{prefix}:{key}:{mode}", prefix_mark=prefix_mark, suffix_text=f" ({q}) • {usd(price)}"))
    kb.button(text="↩️ Назад", callback_data=back_cb)
    kb.adjust(1)
    return kb.as_markup()

def esim_mode_kb(user_id: int | None = None):
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="esim_mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="esim_mode:no_hold")
    kb.button(text="🏠 Закрыть", callback_data="noop")
    kb.adjust(2, 1)
    return kb.as_markup()


def mode_inline_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="mode:no_hold")
    kb.button(text="↩️ Назад", callback_data="menu:submit")
    kb.adjust(2, 1)
    return kb.as_markup()


def mode_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data="mode:hold")
    kb.button(text="⚡ БезХолд", callback_data="mode:no_hold")
    kb.button(text="↩️ Назад", callback_data="mode:back")
    kb.adjust(2, 1)
    return kb.as_markup()

def submit_result_kb(operator_key: str, mode: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="📲 Сдать ещё", callback_data=f"submit_more:{operator_key}:{mode}")
    kb.button(text="✅ Я закончил загрузку", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def admin_queue_kb(item: QueueItem):
    kb = InlineKeyboardBuilder()
    if item.status in {"queued", "taken"}:
        kb.button(text="✅ Встал", callback_data=f"take_start:{item.id}")
        kb.button(text="⚠️ Ошибка", callback_data=f"error_pre:{item.id}")
        kb.adjust(1)
    elif item.status == "in_progress":
        if item.mode == "no_hold":
            kb.button(text="💸 Оплатить", callback_data=f"instant_pay:{item.id}")
        kb.button(text="❌ Слет", callback_data=f"slip:{item.id}")
        kb.adjust(1)
    return kb.as_markup()


def confirm_withdraw_kb(amount: float):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Подтвердить", callback_data=f"withdraw_confirm:{amount}")
    kb.button(text="↩️ Назад", callback_data="withdraw_cancel")
    kb.adjust(1)
    return kb.as_markup()


def withdraw_back_kb():
    return None


def withdraw_admin_kb(withdraw_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Одобрить", callback_data=f"wd_ok:{withdraw_id}")
    kb.button(text="❌ Отклонить", callback_data=f"wd_no:{withdraw_id}")
    kb.adjust(2)
    return kb.as_markup()

def withdraw_paid_kb(withdraw_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Оплачено", callback_data=f"wd_paid:{withdraw_id}")
    kb.adjust(1)
    return kb.as_markup()


def admin_root_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Сводка", callback_data="admin:summary")
    kb.button(text="📈 Стата групп", callback_data="admin:group_stats_panel")
    kb.button(text="💸 Выводы", callback_data="admin:withdraws")
    kb.button(text="🏦 Казна групп", callback_data="admin:group_finance_panel")
    kb.button(text="⏳ Холд", callback_data="admin:hold")
    kb.button(text="💎 Прайсы", callback_data="admin:prices")
    kb.button(text="➕ Добавить оператора", callback_data="admin:add_operator")
    kb.button(text="💎 Эмодзи операторов", callback_data="admin:set_operator_emoji")
    kb.button(text="➖ Удалить оператора", callback_data="admin:remove_operator")
    kb.button(text="👥 Роли", callback_data="admin:roles")
    kb.button(text="🛰 Рабочие зоны", callback_data="admin:workspaces")
    kb.button(text="📦 Очередь", callback_data="admin:queues")
    kb.button(text="🧹 Очистка очередей", callback_data="admin:cleanup_queue")
    kb.button(text="👤 Пользователь", callback_data="admin:user_tools")
    kb.button(text="⚙️ Настройки", callback_data="admin:settings")
    kb.adjust(2)
    return kb.as_markup()


def operator_emoji_pick_kb():
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        kb.add(make_operator_button(operator_key=key, callback_data=f"admin:pick_operator_emoji:{key}"))
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def admin_back_kb(target: str = "admin:home"):
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data=target)
    return kb.as_markup()

def cancel_inline_kb(target: str = "admin:user_tools"):
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data=target)
    kb.adjust(1)
    return kb.as_markup()

def workspace_display_title(chat_id: int, thread_id: int | None = None, chat_title: str | None = None, thread_title: str | None = None) -> str:
    base_title = (chat_title or '').strip()
    if not base_title:
        row = db.conn.execute("SELECT chat_title, thread_title FROM workspaces WHERE chat_id=? AND thread_id=? AND is_enabled=1 ORDER BY id DESC LIMIT 1", (int(chat_id), db._thread_key(thread_id))).fetchone()
        if row:
            if not base_title:
                base_title = (row['chat_title'] or '').strip()
            if not thread_title:
                thread_title = (row['thread_title'] or '').strip()
    if not base_title:
        base_title = str(chat_id)
    if thread_id:
        suffix = (thread_title or '').strip() or f"topic {thread_id}"
        return f"{base_title} / {suffix}"
    return base_title


def set_workspace_title(chat_id: int, thread_id: int | None, chat_title: str | None = None, thread_title: str | None = None):
    try:
        db.conn.execute(
            "UPDATE workspaces SET chat_title=COALESCE(?, chat_title), thread_title=COALESCE(?, thread_title) WHERE chat_id=? AND thread_id=?",
            (chat_title, thread_title, int(chat_id), db._thread_key(thread_id)),
        )
        db.conn.commit()
    except Exception:
        logging.exception("set_workspace_title failed chat_id=%s thread_id=%s", chat_id, thread_id)


def group_stats_list_kb():
    kb = InlineKeyboardBuilder()
    seen = set()
    try:
        rows = db.list_workspaces()
    except Exception:
        rows = db.conn.execute(
            "SELECT chat_id, thread_id, mode, chat_title, thread_title FROM workspaces WHERE is_enabled=1 ORDER BY chat_id DESC, thread_id DESC"
        ).fetchall()

    for row in rows:
        chat_id = int(row["chat_id"])
        raw_thread = row["thread_id"]
        thread_id = None if raw_thread in (None, -1) else int(raw_thread)
        key = (chat_id, thread_id)
        if key in seen:
            continue
        seen.add(key)
        title = workspace_display_title(chat_id, thread_id, row["chat_title"] if "chat_title" in row.keys() else None, row["thread_title"] if "thread_title" in row.keys() else None)
        kb.button(text=(f"💬 {title}")[:52], callback_data=f"admin:groupstat:{chat_id}:{thread_id or 0}")
        kb.button(text="🗑 Удалить", callback_data=f"admin:group_remove:{chat_id}:{thread_id or 0}")

    if not seen:
        kb.button(text="• Пока нет рабочих групп", callback_data="admin:home")
        kb.adjust(1)
    else:
        kb.button(text="↩️ Назад", callback_data="admin:home")
        kb.adjust(2)
    return kb.as_markup()
def group_finance_list_kb():
    kb = InlineKeyboardBuilder()
    seen = set()
    for row in db.list_workspaces():
        chat_id = int(row['chat_id'])
        raw_thread = row['thread_id']
        thread_id = None if raw_thread in (None, -1) else int(raw_thread)
        key = (chat_id, thread_id)
        if key in seen:
            continue
        seen.add(key)
        title = workspace_display_title(chat_id, thread_id, row["chat_title"] if "chat_title" in row.keys() else None, row["thread_title"] if "thread_title" in row.keys() else None)
        label = f"💬 {title}"
        kb.button(text=label[:60], callback_data=f"admin:groupfin:{chat_id}:{thread_id or 0}")
    if not seen:
        kb.button(text="• Пока нет рабочих групп", callback_data="admin:home")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()

def group_finance_manage_kb(chat_id: int, thread_id: int | None):
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Пополнить", callback_data=f"admin:groupfin_add:{chat_id}:{thread_id or 0}")
    kb.button(text="➖ Списать", callback_data=f"admin:groupfin_sub:{chat_id}:{thread_id or 0}")
    for mode in ('hold', 'no_hold'):
        for key in OPERATORS:
            icon = '⏳' if mode == 'hold' else '⚡'
            kb.add(make_operator_button(operator_key=key, callback_data=f"admin:groupprice:{chat_id}:{thread_id or 0}:{mode}:{key}", prefix_mark=f"{icon} "))
    kb.button(text="↩️ К списку групп", callback_data="admin:group_finance_panel")
    kb.adjust(2)
    return kb.as_markup()

def render_single_group_stats(chat_id: int, thread_id: int | None) -> str:
    day_start, day_end, day_label = msk_today_bounds_str()
    thread_key = db._thread_key(thread_id)
    date_expr = "COALESCE(completed_at, work_started_at, taken_at, created_at)"

    totals = db.conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN taken_by_admin IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
            SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS spent_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
        FROM queue_items
        WHERE charge_chat_id=? AND charge_thread_id=? AND {date_expr}>=? AND {date_expr}<?
        """,
        (int(chat_id), thread_key, day_start, day_end),
    ).fetchone()

    per_operator = db.conn.execute(
        f"""
        SELECT
            operator_key,
            COUNT(*) AS total,
            SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
            SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS paid_total,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slip_total,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS error_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS turnover_total
        FROM queue_items
        WHERE charge_chat_id=? AND charge_thread_id=? AND {date_expr}>=? AND {date_expr}<?
        GROUP BY operator_key
        ORDER BY total DESC, operator_key ASC
        """,
        (int(chat_id), thread_key, day_start, day_end),
    ).fetchall()

    per_taker = db.conn.execute(
        f"""
        SELECT
            taken_by_admin AS taker_user_id,
            COUNT(*) AS total,
            SUM(COALESCE(charge_amount, price)) AS turnover_total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_total
        FROM queue_items
        WHERE charge_chat_id=? AND charge_thread_id=? AND {date_expr}>=? AND {date_expr}<? AND taken_by_admin IS NOT NULL
        GROUP BY taken_by_admin
        ORDER BY total DESC
        """,
        (int(chat_id), thread_key, day_start, day_end),
    ).fetchall()

    op_lines = []
    for row in per_operator:
        op_lines.append(
            f"• {op_text(row['operator_key'])}: <b>{int(row['total'] or 0)}</b> "
            f"(✅{int(row['paid_total'] or 0)}/❌{int(row['slip_total'] or 0)}/⚠️{int(row['error_total'] or 0)}) "
            f"на сумму <b>{usd(row['turnover_total'] or 0)}</b>"
        )
    if not op_lines:
        op_lines = ["• Пока пусто"]

    taker_lines = []
    for row in per_taker:
        uid = int(row["taker_user_id"])
        user = db.get_user(uid)
        name = escape(user["full_name"]) if user and user["full_name"] else str(uid)
        taker_lines.append(
            f"• <b>{name}</b> — взял: {int(row['total'] or 0)}, "
            f"успешно: {int(row['completed_total'] or 0)}, "
            f"на сумму: <b>{usd(row['turnover_total'] or 0)}</b>"
        )
    if not taker_lines:
        taker_lines = ["• Пока никто не брал номера"]

    where_label = escape(workspace_display_title(chat_id, thread_id))
    return (
        "<b>📈 Статистика группы за сегодня</b>\n\n"
        f"💬 Группа: <b>{where_label}</b>\n"
        f"🗓 День: <b>{day_label}</b>\n"
        f"♻️ {msk_stats_reset_note()}\n\n"
        f"📦 Взято всего: <b>{int(totals['taken_total'] or 0)}</b>\n"
        f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>\n"
        f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>\n"
        f"💰 Выплачено юзерам: <b>{usd(totals['paid_total'] or 0)}</b>\n"
        f"🏦 Списано с казны: <b>{usd(totals['spent_total'] or 0)}</b>\n"
        f"📈 Маржа группы: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
        "<b>📱 По операторам</b>\n" + "\n".join(op_lines) + "\n\n"
        "<b>👥 Кто сколько взял</b>\n" + "\n".join(taker_lines)
    )

def single_group_stats_kb(chat_id: int, thread_id: int | None):
    kb = InlineKeyboardBuilder()
    kb.button(text="🗑 Убрать группу", callback_data=f"admin:group_remove:{chat_id}:{thread_id or 0}")
    kb.button(text="↩️ К списку групп", callback_data="admin:group_stats_panel")
    kb.adjust(2)
    return kb.as_markup()

def user_price_operator_kb(target_user_id: int):
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        kb.add(make_operator_button(operator_key=key, callback_data=f"admin:user_price_op:{target_user_id}:{key}"))
    kb.button(text="❌ Отмена", callback_data="admin:user_tools")
    kb.adjust(2)
    return kb.as_markup()

def user_price_mode_kb(target_user_id: int, operator_key: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⏳ Холд", callback_data=f"admin:user_price_mode:{target_user_id}:{operator_key}:hold")
    kb.button(text="⚡ БезХолд", callback_data=f"admin:user_price_mode:{target_user_id}:{operator_key}:no_hold")
    kb.button(text="❌ Отмена", callback_data="admin:user_tools")
    kb.adjust(2,1)
    return kb.as_markup()

def user_admin_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Статистика пользователя", callback_data="admin:user_stats")
    kb.button(text="💎 Персональный прайс", callback_data="admin:user_set_price")
    kb.button(text="✉️ Написать в ЛС", callback_data="admin:user_pm")
    kb.button(text="➕ Начислить деньги", callback_data="admin:user_add_balance")
    kb.button(text="➖ Снять деньги", callback_data="admin:user_sub_balance")
    kb.button(text="⛔ Заблокировать", callback_data="admin:user_ban")
    kb.button(text="✅ Разблокировать", callback_data="admin:user_unban")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def queue_manage_kb():
    kb = InlineKeyboardBuilder()
    for item in latest_queue_items(10):
        kb.button(text=f"🗑 #{item['id']} {op_text(item['operator_key'])} {mode_label(item['mode'])}", callback_data=f"admin:queue_remove:{item['id']}")
    kb.button(text="↻ Обновить", callback_data="admin:queues")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def roles_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="👑 Назначить главного", callback_data="admin:role:chief_admin")
    kb.button(text="🛡 Назначить админа", callback_data="admin:role:admin")
    kb.button(text="🎧 Назначить оператора", callback_data="admin:role:operator")
    kb.button(text="🗑 Снять роль", callback_data="admin:role:remove")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def workspaces_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить рабочую группу", callback_data="admin:ws_help_group")
    kb.button(text="➕ Добавить топик", callback_data="admin:ws_help_topic")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def design_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Изменить старт", callback_data="admin:set_start_text")
    kb.button(text="📣 Изменить объявление", callback_data="admin:set_ad_text")
    kb.button(text="🧩 Шаблоны", callback_data="admin:templates")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def broadcast_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Написать рассылку", callback_data="admin:broadcast_write")
    kb.button(text="👀 Превью объявления", callback_data="admin:broadcast_preview")
    kb.button(text="🚀 Разослать объявление", callback_data="admin:broadcast_send_ad")
    kb.button(text="📥 Скачать username", callback_data="admin:usernames")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def escape(value: Optional[str]) -> str:
    return html.escape(str(value or "-"))


def queue_caption(item: QueueItem, price_view: str = "group") -> str:
    """Карточка заявки.
    price_view='group' — для рабочих групп: показывает цену, по которой группа берёт.
    price_view='submit' — для ЛС пользователя: показывает цену, по которой пользователь сдаёт.
    price_view='none' — без цены.
    """
    text = (
        f"📱 {op_html(item.operator_key)}\n\n"
        f"🧾 Заявка: <b>{item.id}</b>\n"
        f"👤 От: <b>{escape(item.full_name)}</b>\n"
        f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
        f"🔄 Режим: <b>{'Холд' if item.mode == 'hold' else 'БезХолд'}</b>"
    )
    try:
        if price_view == "group":
            group_price = getattr(item, 'charge_amount', None)
            if group_price not in (None, 0, 0.0):
                text += f"\n🏷 Прайс группы: <b>{usd(float(group_price))}</b>"
        elif price_view == "submit":
            submit_price = getattr(item, 'price', None)
            if submit_price not in (None, 0, 0.0):
                text += f"\n🏷 Прайс сдачи: <b>{usd(float(submit_price))}</b>"
    except Exception:
        logging.exception("queue_caption price render failed item_id=%s price_view=%s", getattr(item, 'id', None), price_view)
    if item.status == "in_progress":
        text += "\n\n🚀 <b>Работа началась</b>"
        if item.mode == "hold":
            hold_minutes = int(float(db.get_setting("hold_minutes", str(DEFAULT_HOLD_MINUTES))))
            text += (
                f"\n⏳ Холд: <b>{hold_minutes} мин.</b>"
                f"\n📊 {progress_bar(item.hold_until, item.work_started_at)}"
                f"\n⏱ Осталось: <b>{time_left_text(item.hold_until)}</b>"
                f"\n🕓 До: <b>{escape(item.hold_until)}</b>"
            )
        else:
            text += "\n⚡ Режим БезХолд."
    return text

def render_referral(user_id: int) -> str:
    user = db.get_user(user_id)
    try:
        ref_count_row = db.conn.execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=?", (user_id,)).fetchone()
        ref_count = int((ref_count_row['c'] if ref_count_row else 0) or 0)
    except Exception:
        ref_count = 0
    ref_earned = float((user['ref_earned'] if user and 'ref_earned' in user.keys() else 0) or 0)
    link = referral_link(user_id)
    return (
        "<b>🎁 Реферальная система</b>\n\n"
        + quote_block([
            "💸 Вы получаете <b>5%</b> с заработка каждого приглашённого пользователя.",
            f"👥 <b>Ваших рефералов:</b> {ref_count}",
            f"💰 <b>Заработано по рефке:</b> <b>{usd(ref_earned)}</b>",
            f"🔗 <b>Ваша ссылка:</b> <code>{escape(link)}</code>",
        ])
        + "\n\nОтправьте свою ссылку другу. После его старта бот привяжет его к вам автоматически.\n\n"
        + "Награда начисляется, когда реферал получает оплату за успешно сданный номер."
    )

def referral_kb(user_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Обновить", callback_data="menu:ref")
    kb.button(text="👤 Профиль", callback_data="menu:profile")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def render_start(user_id: int) -> str:
    """Главное меню: фото + красивый текст + прайсы/очереди одним caption.

    Telegram caption = 1024 символа, поэтому прайс сделан компактной плашкой.
    """
    now_ts = time.time()
    cached = START_RENDER_CACHE.get(user_id)
    if cached and now_ts - cached[0] <= 15:
        return cached[1]

    user = db.get_user(user_id)
    balance = usd(float(user["balance"] if user else 0))
    username = f"@{escape(user['username'])}" if user and user["username"] else "—"

    title = escape(_strip_html_tags(db.get_setting("start_title", "ESIM Service X")))
    subtitle = escape(_strip_html_tags(db.get_setting("start_subtitle", "Премиум сервис приёма номеров")))

    def money_short(value) -> str:
        try:
            v = float(value)
        except Exception:
            v = 0.0
        if abs(v - int(v)) < 0.001:
            return f"${int(v)}"
        return f"${v:.2f}".rstrip("0").rstrip(".")

    user_price_rows = { (r['operator_key'], r['mode']): r for r in (db.list_user_prices(user_id) or []) }
    queue_counts = cached_visible_operator_queue_counts()
    price_rows: list[str] = []
    for key in visible_operator_keys():
        data = OPERATORS.get(key, {})
        emoji = op_emoji_html(key)
        title_op = escape(str(data.get("title", key)))
        urow_hold = user_price_rows.get((key, "hold"))
        urow_no = user_price_rows.get((key, "no_hold"))
        hp = money_short(urow_hold['price']) if urow_hold else money_short(get_mode_price(key, 'hold', user_id))
        np = money_short(urow_no['price']) if urow_no else money_short(get_mode_price(key, 'no_hold', user_id))
        qh, qn = queue_counts.get(key, (0, 0))
        # Х/БХ = очередь холд / безхолд. Всё в одну ровную строку.
        price_rows.append(f"{emoji} <b>{title_op}</b> — {hp}/{np} · <i>{qh}/{qn}</i>")

    prices_block = "\n".join(price_rows) if price_rows else "Прайсы пока не настроены."

    text = (
        f"💫 <b>{title}</b> 💫\n"
        f"{subtitle}\n\n"
        f"🚀 <b>Быстрый приём заявок</b> • 💎 <b>Стабильные выплаты</b> • 🛡 <b>Контроль статусов</b>\n\n"
        f"🔗 <b>Username:</b> {username}\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"💰 <b>Баланс:</b> <b>{balance}</b>\n\n"
        f"💎 <b>Прайсы и очереди</b> <i>Холд/БезХолд</i>\n"
        f"<blockquote>{prices_block}</blockquote>\n\n"
        f"<b>Вы находитесь в главном меню.</b>\n"
        f"👇 <b>Выберите нужное действие ниже:</b>"
    )

    # Жёсткая страховка: caption должен остаться одним сообщением с фото.
    # Если в БД внезапно длинные названия операторов — убираем лишние жирные теги,
    # но не отправляем вторую плашку.
    if _html_visible_len(text) > 1000:
        price_rows = []
        user_price_rows = {r["operator_key"]: r for r in (db.list_user_prices(user_id) or [])}
        queue_counts = cached_visible_operator_queue_counts()
        for key in visible_operator_keys():
            data = OPERATORS.get(key, {})
            # ВАЖНО: даже в компактном режиме оставляем premium emoji через <tg-emoji>.
            emoji = op_emoji_html(key)
            title_op = escape(_strip_html_tags(str(data.get("title", key))))
            urow_hold = user_price_rows.get((key, "hold"))
            urow_no = user_price_rows.get((key, "no_hold"))
            hp = money_short(urow_hold["price"]) if urow_hold else money_short(get_mode_price(key, "hold", user_id))
            np = money_short(urow_no["price"]) if urow_no else money_short(get_mode_price(key, "no_hold", user_id))
            qh, qn = queue_counts.get(key, (0, 0))
            price_rows.append(f"{emoji} <b>{title_op}</b> — {hp}/{np} · <i>{qh}/{qn}</i>")
        prices_block_compact = "\n".join(price_rows)
        text = (
            f"💫 <b>{title}</b> 💫\n"
            f"{subtitle}\n\n"
            f"🚀 <b>Быстрый приём</b> • 💎 <b>Выплаты</b> • 🛡 <b>Статусы</b>\n\n"
            f"🔗 <b>Username:</b> {username}\n"
            f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
            f"💰 <b>Баланс:</b> <b>{balance}</b>\n\n"
            f"💎 <b>Прайсы и очереди</b> <i>Х/БХ</i>\n"
            f"<blockquote>{prices_block_compact}</blockquote>\n\n"
            f"<b>Вы находитесь в главном меню.</b>\n"
            f"👇 <b>Выберите нужное действие ниже:</b>"
        )

    final = _html_balance_patch(text)
    START_RENDER_CACHE[user_id] = (time.time(), final)
    return final

def render_profile(user_id: int) -> str:
    """Безопасный профиль: не падает, даже если в старой БД не хватает полей/операторов."""
    try:
        user = db.get_user(user_id)
    except Exception:
        logging.exception("profile get_user failed user_id=%s", user_id)
        user = None

    def row_value(row, key, default=0):
        try:
            if row is not None and key in row.keys():
                return row[key]
        except Exception:
            pass
        return default

    try:
        stats = db.user_stats(user_id)
    except Exception:
        logging.exception("profile user_stats failed user_id=%s", user_id)
        stats = None
    try:
        ops = db.user_operator_stats(user_id) or []
    except Exception:
        logging.exception("profile operator_stats failed user_id=%s", user_id)
        ops = []

    current_queue = int((row_value(stats, 'queued', 0) or 0) + (row_value(stats, 'taken', 0) or 0) + (row_value(stats, 'in_progress', 0) or 0))
    username = f"@{escape(row_value(user, 'username', ''))}" if row_value(user, 'username', '') else "—"
    full_name = escape(row_value(user, 'full_name', ''))
    balance = float(row_value(user, 'balance', 0) or 0)
    try:
        payout_link = db.get_payout_link(user_id)
    except Exception:
        logging.exception("profile payout_link failed user_id=%s", user_id)
        payout_link = None
    payout_status = "✅ Привязан" if payout_link else "❌ Не привязан"
    try:
        ref_count_row = db.conn.execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=?", (user_id,)).fetchone()
        ref_count = int((ref_count_row['c'] if ref_count_row else 0) or 0)
    except Exception:
        ref_count = 0
    ref_earned = float(row_value(user, 'ref_earned', 0) or 0)

    op_lines = []
    for row in ops:
        try:
            key = row['operator_key']
            op_lines.append(f"• {op_html(key)}: {int(row['total'] or 0)} шт. / <b>{usd(row['earned'] or 0)}</b>")
        except Exception:
            logging.exception("profile op row render failed user_id=%s", user_id)
    ops_text = "\n".join(op_lines) or "• <i>Пока пусто</i>"

    price_lines = []
    for key, data in list(OPERATORS.items()):
        try:
            price_lines.append(f"{op_emoji_html(key)} <b>{escape(data.get('title', key))}</b> — <b>{usd(get_mode_price(key, 'hold', user_id))}</b> / <b>{usd(get_mode_price(key, 'no_hold', user_id))}</b>")
        except Exception:
            logging.exception("profile price line failed user_id=%s key=%s", user_id, key)
    personal_price_lines = price_lines or ["• <i>Прайсы временно недоступны</i>"]

    try:
        d = user_daily_submit_stats(user_id)
    except Exception:
        logging.exception("profile daily stats failed user_id=%s", user_id)
        d = None

    return (
        "<b>👤 Личный кабинет - ESIM Service X 💫</b>\n\n"
        + quote_block([
            f"🔘 <b>Имя:</b> {full_name}",
            f"™️ <b>Username:</b> {username}",
            f"💲 <b>Баланс:</b> <b>{usd(balance)}</b>",
            f"💳 <b>Счёт CryptoBot:</b> {payout_status}",
        ])
        + "\n\n<b>💎 Ваши прайсы</b>\n"
        + quote_block(personal_price_lines)
        + "\n\n<b>📊 Ваша статистика:</b>\n"
        + quote_block([
            f"🧾 <b>Всего заявок:</b> {int(row_value(stats, 'total', 0) or 0)}",
            f"✅ <b>Успешно:</b> {int(row_value(stats, 'completed', 0) or 0)}",
            f"❌ <b>Слеты:</b> {int(row_value(stats, 'slipped', 0) or 0)}",
            f"⚠️ <b>Ошибки:</b> {int(row_value(stats, 'errors', 0) or 0)}",
            f"💰 <b>Всего заработано:</b> <b>{usd(row_value(stats, 'earned', 0) or 0)}</b>",
            f"📤 <b>Сейчас в очередях:</b> {current_queue}",
        ])
        + "\n\n<b>📆 Статистика за сегодня:</b>\n"
        + quote_block([
            f"📥 <b>Поставлено:</b> {int(row_value(d, 'total', 0) or 0)}",
            f"✅ <b>Успешно:</b> {int(row_value(d, 'completed', 0) or 0)}",
            f"❌ <b>Слеты:</b> {int(row_value(d, 'slipped', 0) or 0)}",
            f"⚠️ <b>Ошибки:</b> {int(row_value(d, 'errors', 0) or 0)}",
            f"💰 <b>Заработано сегодня:</b> <b>{usd(row_value(d, 'earned', 0) or 0)}</b>",
        ])
        + "\n\n<b>🎁 Реферальная система</b>\n"
        + quote_block([
            f"👥 <b>Рефералов:</b> {ref_count}",
            f"💸 <b>Реф. доход:</b> <b>{usd(ref_earned)}</b>",
            f"🔗 <b>Ссылка:</b> <code>{escape(referral_link(user_id))}</code>",
        ])
        + "\n\n<b>📱 Разбивка по операторам</b>\n"
        + quote_block([ops_text])
        + "\n\n<i>Профиль обновляется автоматически по мере работы в боте.</i>"
    )
def render_withdraw(user_id: int) -> str:
    user = db.get_user(user_id)
    balance = usd(float(user['balance'] if user else 0))
    minimum = usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))
    return (
        "<b>💸 Вывод средств - ESIM Service X 💫</b>\n\n"
        + quote_block([
            f"🔻 <b>Минимальный вывод:</b> {minimum}",
            f"💰 <b>Ваш баланс:</b> {balance}",
        ])
        + "\n\n🔹 <b>Введите сумму вывода в $:</b>"
    )

def render_withdraw_setup() -> str:
    return (
        "<b>Вывод средств - ESIM Service X 💫</b>\n\n"
        "<b>💳 Настройка оплаты (CryptoBot)</b>\n\n"
        "Для получения выплат мне необходима ваша ссылка на многоразовый счет.\n\n"
        "<b>Инструкция:</b>\n"
        "Способ 1: напишите <b>@send</b> и выберите <b>Создать многоразовый счет</b>. Сумму не указывайте.\n\n"
        "Способ 2: В <b>@CryptoBot</b> пропишите <code>/invoices</code> — Создать счёт — Многоразовый — USDT — Далее и скопируйте ссылку.\n\n"
        "👉 <b>Просто отправьте скопированную ссылку прямо мне в чат, и я её запомню.</b>"
    )

def render_my_numbers(user_id: int, page: int = 0) -> str:
    items = user_active_queue_items(user_id)
    total = len(items or [])
    max_page = max((total - 1) // MY_NUMBERS_PAGE_SIZE, 0)
    page = max(0, min(int(page or 0), max_page))
    if not items:
        body = "• Активных заявок пока нет."
    else:
        rows = []
        start = page * MY_NUMBERS_PAGE_SIZE
        for row in items[start:start + MY_NUMBERS_PAGE_SIZE]:
            pos = queue_position(row['id']) if row['status'] == 'queued' else None
            pos_text = f" • <b>позиция:</b> {pos}" if pos else ""
            rows.append(
                f"#{row['id']} • {op_text(row['operator_key'])} • <b>{mode_label(row['mode'])}</b>\n"
                f"{pretty_phone(row['normalized_phone'])} • 🏷 <b>{usd(float(row['price'] or 0))}</b> • "
                f"<b>{status_label_from_row(row)}</b>{pos_text}"
            )
        body = "\n".join(rows) or "• На этой странице пусто."
    page_line = f"\n\n<b>Страница:</b> {page + 1}/{max_page + 1} • <b>Всего активных:</b> {total}" if total else ""
    return (
        "<b>📦 Мои номера — активные</b>\n\n"
        + quote_block([body])
        + page_line
        + "\n\n<i>Здесь видны номера, которые ещё стоят в очереди, взяты или в работе. Они не пропадают в 00:00, пока вы сами их не уберёте или их не обработают.</i>"
    )

def render_mirror_menu(user_id: int) -> str:
    rows = db.user_mirrors(user_id)
    if rows:
        body = "\n".join(
            f"• @{escape(row['bot_username'] or 'unknown_bot')} — <b>{'запущено' if row['status'] == 'active' else escape(row['status'])}</b>"
            for row in rows
        )
    else:
        body = "• Пока зеркал нет."
    return (
        "<b>🪞 Зеркало бота</b>\n\n"
        "Здесь можно сохранить токен нового бота от <b>@BotFather</b> и подготовить зеркало.\n"
        "Зеркало не даёт владельцу никаких админ-прав и работает на общей базе.\n\n"
        "<b>Ваши зеркала:</b>\n"
        + body
    )


def render_group_stats_panel() -> str:
    day_start, day_end, day_label = msk_today_bounds_str()
    date_expr = "COALESCE(completed_at, work_started_at, taken_at, created_at)"
    totals = db.conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN taken_by_admin IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
            SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS turnover_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
        FROM queue_items
        WHERE charge_chat_id IS NOT NULL AND {date_expr}>=? AND {date_expr}<?
        """,
        (day_start, day_end),
    ).fetchone()

    per_operator = db.conn.execute(
        f"""
        SELECT
            operator_key,
            COUNT(*) AS total,
            SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
            SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS paid_total,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slip_total,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS error_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS turnover_total
        FROM queue_items
        WHERE charge_chat_id IS NOT NULL AND {date_expr}>=? AND {date_expr}<?
        GROUP BY operator_key
        ORDER BY total DESC, operator_key ASC
        """,
        (day_start, day_end),
    ).fetchall()

    per_taker = db.conn.execute(
        f"""
        SELECT
            taken_by_admin AS taker_user_id,
            COUNT(*) AS total,
            SUM(COALESCE(charge_amount, price)) AS turnover_total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed_total
        FROM queue_items
        WHERE charge_chat_id IS NOT NULL AND {date_expr}>=? AND {date_expr}<? AND taken_by_admin IS NOT NULL
        GROUP BY taken_by_admin
        ORDER BY total DESC
        """,
        (day_start, day_end),
    ).fetchall()

    op_lines = []
    for row in per_operator:
        op_lines.append(
            f"• {op_text(row['operator_key'])}: <b>{int(row['total'] or 0)}</b> "
            f"(✅{int(row['paid_total'] or 0)}/❌{int(row['slip_total'] or 0)}/⚠️{int(row['error_total'] or 0)}) "
            f"на сумму <b>{usd(row['turnover_total'] or 0)}</b>"
        )
    if not op_lines:
        op_lines = ["• Пока пусто"]

    taker_lines = []
    for row in per_taker:
        uid = int(row["taker_user_id"])
        user = db.get_user(uid)
        name = escape(user["full_name"]) if user and user["full_name"] else str(uid)
        taker_lines.append(
            f"• <b>{name}</b> — взял: {int(row['total'] or 0)}, "
            f"успешно: {int(row['completed_total'] or 0)}, "
            f"на сумму: <b>{usd(row['turnover_total'] or 0)}</b>"
        )
    if not taker_lines:
        taker_lines = ["• Пока никто не брал номера"]

    return (
        "<b>📈 Стата групп за сегодня</b>\n\n"
        f"🗓 День: <b>{day_label}</b>\n"
        f"♻️ {msk_stats_reset_note()}\n\n"
        f"📦 Всего заявок в рабочих группах: <b>{int(totals['total'] or 0)}</b>\n"
        f"🙋 Взято: <b>{int(totals['taken_total'] or 0)}</b>\n"
        f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>\n"
        f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>\n"
        f"💰 Выплачено юзерам: <b>{usd(totals['paid_total'] or 0)}</b>\n"
        f"🏦 Общий оборот: <b>{usd(totals['turnover_total'] or 0)}</b>\n"
        f"📈 Общая маржа: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
        "<b>📱 По операторам</b>\n" + "\n".join(op_lines) + "\n\n"
        "<b>👥 Кто сколько взял</b>\n" + "\n".join(taker_lines)
    )

def render_admin_home() -> str:
    return (
        "<b>⚙️ Admin Panel — ESIM Service X</b>\n\n"
        f"👑 Главный админ: <code>{CHIEF_ADMIN_ID}</code>\n"
        f"💸 Заявок на вывод: <b>{db.count_pending_withdrawals()}</b>\n"
        f"⏳ Холд: <b>{db.get_setting('hold_minutes')}</b> мин.\n"
        f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
        f"📥 Сдача номеров: <b>{'Включена' if is_numbers_enabled() else 'Выключена'}</b>\n"
        f"🔐 Ваша роль: <b>{user_role(CHIEF_ADMIN_ID)}</b>"
    )


def summary_stats_for_period(day_start: str, day_end: str):
    submitted = db.conn.execute(
        "SELECT COUNT(*) AS submitted_total FROM queue_items WHERE (created_at>=? AND created_at<?) OR status IN ('queued','taken','in_progress','waiting_check','checking','on_hold')",
        (day_start, day_end),
    ).fetchone()
    actions = db.conn.execute(
        f"""
        SELECT
            SUM(CASE WHEN taken_at IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS paid_total,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips_total,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
        FROM queue_items
        WHERE (created_at>=? AND created_at<?) OR status IN ('queued','taken','in_progress','waiting_check','checking','on_hold')
        """,
        (day_start, day_end),
    ).fetchone()
    return {
        'submitted_total': int((submitted['submitted_total'] if submitted else 0) or 0),
        'taken_total': int((actions['taken_total'] if actions else 0) or 0),
        'paid_total': int((actions['paid_total'] if actions else 0) or 0),
        'slips_total': int((actions['slips_total'] if actions else 0) or 0),
        'errors_total': int((actions['errors_total'] if actions else 0) or 0),
        'margin_total': float((actions['margin_total'] if actions else 0) or 0),
    }


def render_admin_summary_for_date(day_start: str, day_end: str, day_label: str) -> str:
    totals = db.conn.execute(
        f"""
        SELECT
            COUNT(*) AS submitted_total,
            SUM(CASE WHEN taken_at IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS paid_total,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips_total,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors_total,
            SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
        FROM queue_items
        WHERE (created_at>=? AND created_at<?) OR status IN ('queued','taken','in_progress','waiting_check','checking','on_hold')
        """,
        (day_start, day_end),
    ).fetchone()
    daily = summary_stats_for_period(day_start, day_end)
    lines = []
    for key, data in OPERATORS.items():
        lines.append(f"• {op_text(key)}: {db.count_waiting(key)}")
    return (
        "<b>📊 Общая сводка</b>\n\n"
        f"📥 Сдано номеров: <b>{int(totals['submitted_total'] or 0)}</b>\n"
        f"🙋 Взято в работу: <b>{int(totals['taken_total'] or 0)}</b>\n"
        f"✅ Оплачено: <b>{int(totals['paid_total'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(totals['slips_total'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(totals['errors_total'] or 0)}</b>\n"
        f"📈 Маржа: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
        f"<b>🗓 Отчет за дату — {day_label}</b>\n"
        f"📥 Сдано: <b>{daily['submitted_total']}</b> • "
        f"🙋 Взято: <b>{daily['taken_total']}</b> • "
        f"✅ Оплачено: <b>{daily['paid_total']}</b>\n"
        f"❌ Слеты: <b>{daily['slips_total']}</b> • "
        f"⚠️ Ошибки: <b>{daily['errors_total']}</b> • "
        f"📈 Маржа: <b>{usd(daily['margin_total'])}</b>\n\n"
        "<b>📦 Очередь по операторам</b>\n" + "\n".join(lines)
    )


def render_admin_summary() -> str:
    day_start, day_end, day_label = msk_today_bounds_str()
    return render_admin_summary_for_date(day_start, day_end, day_label)


def admin_summary_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Отчет по дате", callback_data="admin:summary_by_date")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


def render_admin_treasury() -> str:
    recent = db.list_recent_treasury_invoices(5)
    extra = ""
    if recent:
        extra = "\n\n<b>Последние инвойсы:</b>\n" + "\n".join(
            f"• #{row['id']} — {usd(row['amount'])} — <b>{row['status']}</b>" for row in recent
        )
    return f"<b>🏦 Казна</b>\n\n💰 Баланс казны: <b>{usd(db.get_treasury())}</b>{extra}"


def render_admin_withdraws() -> str:
    return f"<b>💸 Выводы</b>\n\n📬 В ожидании: <b>{db.count_pending_withdrawals()}</b>"


def render_admin_hold() -> str:
    return f"<b>⏳ Холд</b>\n\nТекущее время Холд: <b>{db.get_setting('hold_minutes')}</b> мин."


def render_admin_settings() -> str:
    return (
        "<b>⚙️ Настройки системы</b>\n\n"
        f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
        f"📥 Приём номеров: <b>{'Включен' if is_numbers_enabled() else 'Выключен'}</b>\n"
        f"📝 Старт-заголовок: <b>{escape(db.get_setting('start_title', 'ESIM Service X'))}</b>\n"
        f"💸 Канал выплат: <code>{escape(db.get_setting('withdraw_channel_id', str(WITHDRAW_CHANNEL_ID)))}</code>\n"
        f"🧵 Топик выплат: <code>{escape(db.get_setting('withdraw_thread_id', '0'))}</code>\n"
        f"🧾 Канал логов: <code>{escape(db.get_setting('log_channel_id', str(LOG_CHANNEL_ID)))}</code>\n"
        f"👥 Обяз. группа: <code>{escape(db.get_setting('required_join_chat_id', '0'))}</code>\n"
        f"🔗 Ссылка вступления: <code>{escape(db.get_setting('required_join_link', ''))}</code>\n"
        f"🗄 Канал автобэкапа: <code>{escape(db.get_setting('backup_channel_id', '0'))}</code>\n"
        f"🏷 Общий прайс для групп: <b>{'задан' if db.get_setting('group_default_price_hold_mts', None) is not None else 'не задан'}</b>\n"
        f"📱 Операторов в системе: <b>{len(OPERATORS)}</b>\n"
        f"🔁 Автовыгрузка БД: <b>{'Включена' if is_backup_enabled() else 'Выключена'}</b>\n"
        f"📣 Рассылка: <b>{'задана' if db.get_setting('broadcast_text', '').strip() else 'пусто'}</b>"
    )

def render_operator_modes() -> str:
    lines = [f"📥 <b>Общий приём номеров:</b> {'✅ Включен' if is_numbers_enabled() else '🚫 Выключен'}", ""]
    for key in OPERATORS:
        hold_status = "✅" if is_operator_mode_enabled(key, "hold") else "🚫"
        nh_status = "✅" if is_operator_mode_enabled(key, "no_hold") else "🚫"
        lines.append(f"{op_text(key)}\n• Холд: {hold_status}\n• БезХолд: {nh_status}")
    return "<b>🎛 Приём номеров по операторам</b>\n\n" + "\n\n".join(lines)

def hold_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Изменить время Холд", callback_data="admin:set_hold")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()

def prices_kb():
    kb = InlineKeyboardBuilder()
    for mode in ("hold", "no_hold"):
        mode_label_text = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
        for key in OPERATORS:
            kb.add(make_operator_button(operator_key=key, callback_data=f"admin:set_price:{mode}:{key}", prefix_mark=f"{mode_label_text} • "))
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()

def group_default_prices_kb():
    kb = InlineKeyboardBuilder()
    for mode in ("hold", "no_hold"):
        mode_label_text = "🏷 Группы ⏳" if mode == "hold" else "🏷 Группы ⚡"
        for key in OPERATORS:
            cur = group_default_price_for_take(key, mode)
            kb.add(make_operator_button(operator_key=key, callback_data=f"admin:set_group_default_price:{mode}:{key}", prefix_mark=f"{mode_label_text} • ", suffix_text=f" • {usd(cur)}"))
    kb.button(text="↩️ Назад", callback_data="admin:settings")
    kb.adjust(2)
    return kb.as_markup()

def render_group_default_prices() -> str:
    lines = ["<b>🏷 Общий прайс для групп</b>", "", "Цены по умолчанию для всех групп, если для группы нет отдельного прайса."]
    for key in OPERATORS:
        lines.append(f"• {op_text(key)} — ⏳ {usd(group_default_price_for_take(key, 'hold'))} / ⚡ {usd(group_default_price_for_take(key, 'no_hold'))}")
    return "\n".join(lines)

def settings_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Мин. вывод", callback_data="admin:set_min_withdraw")
    kb.button(text="📥 Вкл/Выкл приём номеров", callback_data="admin:toggle_numbers")
    kb.button(text="🎛 Приём номеров по операторам", callback_data="admin:operator_modes")
    kb.button(text="✍️ Старт-текст", callback_data="admin:set_start_text")
    kb.button(text="📣 Рассылка", callback_data="admin:broadcast")
    kb.button(text="💳 Канал выплат", callback_data="admin:set_withdraw_channel")
    kb.button(text="🧵 Топик выплат", callback_data="admin:set_withdraw_topic")
    kb.button(text="🧾 Канал логов", callback_data="admin:set_log_channel")
    kb.button(text="👥 Обяз. подписка", callback_data="admin:required_join_manage")
    kb.button(text="🗄 Канал автобэкапа", callback_data="admin:set_backup_channel")
    kb.button(text="🏷 Общий прайс для групп", callback_data="admin:group_default_prices")
    kb.button(text="🔁 Автовыгрузка БД", callback_data="admin:toggle_backup")
    kb.button(text="📤 Скачать БД", callback_data="admin:download_db")
    kb.button(text="📥 Загрузить БД", callback_data="admin:upload_db")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(2)
    return kb.as_markup()


async def ask_admin_channel_setting(callback: CallbackQuery, state: FSMContext, setting_key: str, label: str):
    await state.set_state(AdminStates.waiting_channel_value)
    await state.update_data(channel_target=setting_key)
    await callback.message.answer(
        f"{label}\n\n"
        "Отправьте <b>ID канала числом</b>.\n"
        "Для очистки отправьте <code>0</code>."
    )
    await safe_callback_answer(callback)

def required_join_manage_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить канал", callback_data="admin:required_join_add")
    kb.button(text="➖ Убрать канал", callback_data="admin:required_join_remove")
    kb.button(text="🧹 Очистить все", callback_data="admin:required_join_clear")
    kb.button(text="↩️ Назад", callback_data="admin:settings")
    kb.adjust(2, 1, 1)
    return kb.as_markup()

def operator_modes_kb():
    kb = InlineKeyboardBuilder()
    for mode in ("hold", "no_hold"):
        mode_label_text = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
        for key in OPERATORS:
            status = "✅" if is_operator_mode_enabled(key, mode) else "🚫"
            kb.add(make_operator_button(operator_key=key, callback_data=f"admin:toggle_avail:{mode}:{key}", prefix_mark=f"{status} {mode_label_text} • "))
    kb.button(text="↩️ Назад", callback_data="admin:settings")
    kb.adjust(2)
    return kb.as_markup()



def render_design() -> str:
    return (
        "<b>🎨 Дизайн и тексты</b>\n\n"
        f"🪪 Заголовок: <b>{escape(db.get_setting('start_title', 'DIAMOND HUB'))}</b>\n"
        f"💬 Подзаголовок: <b>{escape(db.get_setting('start_subtitle', ''))}</b>\n"
        f"📣 Рассылка: <b>{'есть' if db.get_setting('announcement_text', '').strip() else 'нет'}</b>\n\n"
        "Здесь можно менять оформление главного экрана и текст рассылки.\n"
        "Поддерживается HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
    )


def render_templates() -> str:
    return (
        "<b>🧩 Шаблоны для объявлений</b>\n\n"
        "<b>Шаблон 1 — премиум:</b>\n"
        "<code>&lt;b&gt;💎 DIAMOND HUB&lt;/b&gt;\n&lt;i&gt;Премиум сервис приёма номеров&lt;/i&gt;\n\n🚀 Быстрый старт • 💰 Выплаты • 🛡 Контроль&lt;/code&gt;\n\n"
        "<b>Шаблон 2 — рассылка:</b>\n"
        "<code>&lt;b&gt;📣 Новое объявление&lt;/b&gt;\n\n• пункт 1\n• пункт 2\n• пункт 3&lt;/code&gt;\n\n"
        "<b>Шаблон 3 — оффер:</b>\n"
        "<code>&lt;b&gt;⚡ Акция дня&lt;/b&gt;\n&lt;blockquote&gt;Короткое описание предложения&lt;/blockquote&gt;&lt;/code&gt;"
    )


def render_broadcast() -> str:
    count = len(db.all_user_ids())
    return (
        "<b>📣 Объявления и рассылки</b>\n\n"
        f"👥 База пользователей: <b>{count}</b>\n"
        f"🔗 Username собрано: <b>{sum(1 for line in db.export_usernames().splitlines() if line.startswith('@'))}</b>\n\n"
        "Здесь можно написать красивое объявление, сохранить его и разослать всем пользователям."
    )


def render_admin_prices() -> str:
    hold_lines = [f"• {op_text(key)}: <b>{usd(get_mode_price(key, 'hold'))}</b>" for key, data in OPERATORS.items()]
    no_hold_lines = [f"• {op_text(key)}: <b>{usd(get_mode_price(key, 'no_hold'))}</b>" for key, data in OPERATORS.items()]
    return "<b>💎 Прайсы</b>\n\n<b>⏳ Холд</b>\n" + "\n".join(hold_lines) + "\n\n<b>⚡ БезХолд</b>\n" + "\n".join(no_hold_lines)


def render_roles() -> str:
    rows = db.list_roles()
    body = []
    for row in rows:
        emoji = "👑" if row["role"] == "chief_admin" else "🛡" if row["role"] == "admin" else "🎧"
        body.append(f"{emoji} <code>{row['user_id']}</code> — <b>{row['role']}</b>")
    return "<b>👥 Роли</b>\n\n" + ("\n".join(body) if body else "Пока пусто")


def render_workspaces() -> str:
    rows = db.list_workspaces()
    if not rows:
        body = "Нет активных рабочих зон.\n\n• /work — включить или выключить группу\n• /topic — включить или выключить топик"
    else:
        body = "\n".join(
            f"• chat <code>{row['chat_id']}</code> | thread <code>{0 if row['thread_id'] in (None, -1) else row['thread_id']}</code> | {row['mode']}"
            for row in rows
        )
    return "<b>🛰 Рабочие зоны</b>\n\n" + body




def mode_label(mode: str) -> str:
    return "Холд" if mode == "hold" else "БезХолд"


def mode_emoji(mode: str) -> str:
    return "⏳" if mode == "hold" else "⚡"


def status_label(status: str, fail_reason: Optional[str] = None, mode: str | None = None) -> str:
    """Красивые статусы для пользователя.

    БХ: очередь -> В очереди, взяли -> В обработке, встал -> На проверке, оплата -> Оплата.
    Холд: очередь -> В очереди, взяли -> В обработке, встал -> На холде.
    """
    status = str(status or "").lower()
    mode = str(mode or "").lower()
    fail_reason = str(fail_reason or "")
    if status in ("queued", "new"):
        return "В очереди"
    if status == "taken":
        return "В обработке"
    if status in ("in_progress", "waiting_check", "checking"):
        return "На холде" if mode == "hold" else "На проверке"
    if status in ("on_hold", "hold"):
        return "На холде"
    if status == "completed":
        return "Оплата"
    if status == "failed":
        if fail_reason and "error" in fail_reason:
            return "Ошибка"
        if fail_reason == "slip":
            return "Слет"
        if fail_reason == "admin_removed":
            return "Удалено админом"
        if fail_reason == "user_removed":
            return "Удалено пользователем"
        return "Неуспешно"
    return status or "В очереди"

def status_label_from_row(row) -> str:
    return status_label(
        row["status"],
        row["fail_reason"] if "fail_reason" in row.keys() else None,
        row["mode"] if "mode" in row.keys() else None,
    )


ACTIVE_DISPLAY_QUEUE_STATUSES = ('queued', 'taken', 'in_progress', 'waiting_check', 'checking', 'on_hold')

def visible_operator_queue_counts() -> dict[str, tuple[int, int]]:
    keys = visible_operator_keys()
    if not keys:
        return {}
    placeholders = ",".join(["?"] * len(keys))
    rows = db.conn.execute(
        f"""
        SELECT operator_key,
               SUM(CASE WHEN mode='hold' AND status='queued' THEN 1 ELSE 0 END) AS hold_q,
               SUM(CASE WHEN mode='no_hold' AND status='queued' THEN 1 ELSE 0 END) AS no_hold_q
          FROM queue_items
         WHERE operator_key IN ({placeholders})
           AND status='queued'
         GROUP BY operator_key
        """,
        tuple(keys),
    ).fetchall()
    return {row["operator_key"]: (int(row["hold_q"] or 0), int(row["no_hold_q"] or 0)) for row in rows}


def cached_visible_operator_queue_counts(ttl_seconds: int = 20) -> dict[str, tuple[int, int]]:
    global QUEUE_COUNTS_CACHE
    now_ts = time.time()
    if QUEUE_COUNTS_CACHE is not None:
        ts, data = QUEUE_COUNTS_CACHE
        if now_ts - ts <= ttl_seconds:
            return data
    data = visible_operator_queue_counts()
    QUEUE_COUNTS_CACHE = (now_ts, data)
    return data

def looks_like_payout_link(raw: str) -> bool:
    raw = (raw or "").strip()
    lowered = raw.lower()
    patterns = [
        "t.me/send?start=",
        "https://t.me/send?start=",
        "http://t.me/send?start=",
        "telegram.me/send?start=",
        "https://telegram.me/send?start=",
        "t.me/cryptobot?start=",
        "https://t.me/cryptobot?start=",
        "t.me/cryptobot/app?startapp=",
        "https://t.me/cryptobot/app?startapp=",
        "app.send.tg",
        "send.tg",
        "send?start=iv",
        "start=iv",
        "startapp=invoice",
        "invoice",
    ]
    if any(p in lowered for p in patterns):
        return True
    if "@send" in lowered or "@cryptobot" in lowered:
        return True
    if ("t.me/" in lowered or "telegram.me/" in lowered) and ("start=" in lowered or "startapp=" in lowered):
        return True
    return False


def msk_day_window() -> tuple[str, str]:
    now = msk_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return fmt_dt(start), fmt_dt(end)


def ensure_extra_schema():
    cur = db.conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS custom_operators (
            key TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            price REAL NOT NULL DEFAULT 0,
            command TEXT NOT NULL,
            emoji_id TEXT DEFAULT '',
            emoji TEXT DEFAULT '📱',
            is_deleted INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)
    user_cols = {r['name'] for r in cur.execute("PRAGMA table_info(users)").fetchall()}
    if 'is_blocked' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER NOT NULL DEFAULT 0")
    if 'last_seen_at' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_seen_at TEXT")
    if 'referred_by' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
    if 'ref_earned' not in user_cols:
        cur.execute("ALTER TABLE users ADD COLUMN ref_earned REAL NOT NULL DEFAULT 0")
    wd_cols = {r['name'] for r in cur.execute("PRAGMA table_info(withdrawals)").fetchall()}
    ws_cols = {r['name'] for r in cur.execute("PRAGMA table_info(workspaces)").fetchall()}
    qi_cols = {r['name'] for r in cur.execute("PRAGMA table_info(queue_items)").fetchall()}
    if 'qr_blob' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN qr_blob BLOB")
    if 'qr_mime' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN qr_mime TEXT")
    if 'qr_filename' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN qr_filename TEXT")
    if 'submit_bot_token' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN submit_bot_token TEXT")
    if 'charge_chat_id' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_chat_id INTEGER")
    if 'charge_thread_id' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_thread_id INTEGER")
    if 'charge_amount' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_amount REAL")
    if 'user_hold_chat_id' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN user_hold_chat_id INTEGER")
    if 'user_hold_message_id' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN user_hold_message_id INTEGER")
    if 'charge_refunded' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN charge_refunded INTEGER NOT NULL DEFAULT 0")
    if 'operator_title_snapshot' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN operator_title_snapshot TEXT")
    if 'operator_command_snapshot' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN operator_command_snapshot TEXT")
    if 'operator_emoji_id_snapshot' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN operator_emoji_id_snapshot TEXT")
    if 'operator_emoji_snapshot' not in qi_cols:
        cur.execute("ALTER TABLE queue_items ADD COLUMN operator_emoji_snapshot TEXT")
    if 'chat_title' not in ws_cols:
        cur.execute("ALTER TABLE workspaces ADD COLUMN chat_title TEXT")
    if 'thread_title' not in ws_cols:
        cur.execute("ALTER TABLE workspaces ADD COLUMN thread_title TEXT")
    if 'payout_check_id' not in wd_cols:
        cur.execute("ALTER TABLE withdrawals ADD COLUMN payout_check_id INTEGER")
    defaults = {
        'numbers_enabled': '1',
        'start_banner_path': START_BANNER,
        'profile_banner_path': PROFILE_BANNER,
        'my_numbers_banner_path': MY_NUMBERS_BANNER,
        'withdraw_banner_path': WITHDRAW_BANNER,
        'withdraw_channel_id': str(WITHDRAW_CHANNEL_ID),
        'log_channel_id': str(LOG_CHANNEL_ID),
    }
    for mode in ('hold','no_hold'):
        for key,data in OPERATORS.items():
            defaults[f'price_{mode}_{key}'] = str(data['price'])
    for k,v in defaults.items():
        cur.execute("INSERT OR IGNORE INTO settings(key,value) VALUES (?,?)", (k,v))
    try:
        cur.execute("UPDATE workspaces SET thread_id=-1 WHERE thread_id IS NULL")
    except Exception:
        pass
    try:
        for old_key, new_key in OPERATOR_KEY_ALIASES.items():
            cur.execute("UPDATE queue_items SET operator_key=? WHERE operator_key=?", (new_key, old_key))
            old_title = db.get_setting(f"operator_title_{old_key}", None)
            if old_title and not db.get_setting(f"operator_title_{new_key}", None):
                db.set_setting(f"operator_title_{new_key}", old_title)
            for mode in ("hold", "no_hold"):
                old_price = db.get_setting(f"price_{mode}_{old_key}", None)
                if old_price is not None and db.get_setting(f"price_{mode}_{new_key}", None) is None:
                    db.set_setting(f"price_{mode}_{new_key}", old_price)
    except Exception:
        logging.exception("operator alias migration failed")
    db.conn.commit()


ensure_extra_schema()


def _normalize_operator_payload(item: dict):
    key = str(item.get('key', '')).strip().lower()
    key = re.sub(r'[^a-z0-9_]+', '', key)
    key = OPERATOR_KEY_ALIASES.get(key, key)
    title = str(item.get('title', '')).strip()
    if not title:
        title = key.upper() if key else ''
    if not key or not title:
        return None
    try:
        price = float(item.get('price', 0) or 0)
    except Exception:
        price = 0.0
    command = str(item.get('command', f'/{key}') or f'/{key}').strip()
    if not command.startswith('/'):
        command = '/' + command
    emoji_id = str(item.get('emoji_id', '') or '').strip()
    fallback_emoji = str(item.get('emoji', item.get('fallback', '📱')) or '📱')[:2]
    return {'key': key, 'title': title, 'price': price, 'command': command, 'emoji_id': emoji_id, 'emoji': fallback_emoji or '📱'}


def upsert_custom_operator_store(key: str, title: str, price: float, command: str = None, emoji_id: str = '', emoji: str = '📱'):
    key = re.sub(r'[^a-z0-9_]+', '', str(key or '').strip().lower())
    key = OPERATOR_KEY_ALIASES.get(key, key)
    if not key:
        return
    command = command or f'/{key}'
    if not command.startswith('/'):
        command = '/' + command
    try:
        price = float(price or 0)
    except Exception:
        price = 0.0
    title = str(title or key.upper()).strip()
    emoji_id = str(emoji_id or '').strip()
    emoji = str(emoji or '📱')[:2] or '📱'
    db.conn.execute(
        """
        INSERT INTO custom_operators(key,title,price,command,emoji_id,emoji,is_deleted,updated_at)
        VALUES(?,?,?,?,?,?,0,?)
        ON CONFLICT(key) DO UPDATE SET
            title=excluded.title,
            price=excluded.price,
            command=excluded.command,
            emoji_id=excluded.emoji_id,
            emoji=excluded.emoji,
            is_deleted=0,
            updated_at=excluded.updated_at
        """,
        (key, title, price, command, emoji_id, emoji, now_str()),
    )
    db.set_setting(f'operator_title_{key}', title)
    db.set_setting(f'operator_command_{key}', command)
    db.set_setting(f'operator_emoji_id_{key}', emoji_id)
    db.set_setting(f'operator_emoji_{key}', emoji)
    db.set_setting(f'price_{key}', str(price))
    db.set_setting(f'price_hold_{key}', str(price))
    db.set_setting(f'price_no_hold_{key}', str(price))
    db.set_setting(f'allow_hold_{key}', db.get_setting(f'allow_hold_{key}', '1'))
    db.set_setting(f'allow_no_hold_{key}', db.get_setting(f'allow_no_hold_{key}', '1'))
    # keep old json mirror too, so older code/backups still see custom operators
    items = load_extra_operator_items()
    payload = {'key': key, 'title': title, 'price': price, 'command': command, 'emoji_id': emoji_id, 'emoji': emoji}
    found = False
    for item in items:
        if isinstance(item, dict) and str(item.get('key','')).strip().lower() == key:
            item.update(payload)
            found = True
            break
    base_keys = set(visible_operator_keys())
    if not found and key not in base_keys:
        items.append(payload)
    db.set_setting('extra_operators_json', json.dumps(items, ensure_ascii=False))
    OPERATORS[key] = {'title': title, 'price': price, 'command': command}
    CUSTOM_OPERATOR_EMOJI[key] = (emoji_id, emoji)


def load_extra_operators_from_settings():
    items_by_key = {}
    # 1) old json storage
    raw = db.get_setting('extra_operators_json', '[]') or '[]'
    try:
        raw_items = json.loads(raw)
    except Exception:
        raw_items = []
    if isinstance(raw_items, list):
        for item in raw_items:
            if isinstance(item, dict):
                payload = _normalize_operator_payload(item)
                if payload:
                    items_by_key[payload['key']] = payload
    # 2) new reliable table storage
    try:
        rows = db.conn.execute("SELECT * FROM custom_operators WHERE COALESCE(is_deleted,0)=0").fetchall()
        for r in rows:
            payload = _normalize_operator_payload(dict(r))
            if payload:
                items_by_key[payload['key']] = payload
    except Exception as e:
        logging.exception("custom_operators load failed: %s", e)
    # 3) recovery: if DB already has numbers on a custom operator, restore key so old numbers don't disappear
    try:
        rows = db.conn.execute("SELECT DISTINCT operator_key FROM queue_items WHERE operator_key IS NOT NULL AND operator_key != ''").fetchall()
        for r in rows:
            key = str(r['operator_key'] or '').strip().lower()
            if not key or key in OPERATORS or key in items_by_key:
                continue
            title = db.get_setting(f'operator_title_{key}', key.upper())
            price_raw = db.get_setting(f'price_hold_{key}', db.get_setting(f'price_no_hold_{key}', db.get_setting(f'price_{key}', None)))
            if price_raw is None:
                pr = db.conn.execute("SELECT price FROM queue_items WHERE operator_key=? AND price IS NOT NULL ORDER BY id DESC LIMIT 1", (key,)).fetchone()
                price_raw = str(pr['price'] if pr and pr['price'] is not None else 0)
            items_by_key[key] = {
                'key': key,
                'title': title,
                'price': float(price_raw or 0),
                'command': db.get_setting(f'operator_command_{key}', f'/{key}'),
                'emoji_id': db.get_setting(f'operator_emoji_id_{key}', ''),
                'emoji': db.get_setting(f'operator_emoji_{key}', '📱'),
            }
            logging.warning("Recovered missing custom operator from queue_items: key=%s title=%s", key, title)
    except Exception as e:
        logging.exception("recover operators from queue_items failed: %s", e)
    for payload in items_by_key.values():
        upsert_custom_operator_store(**payload)


def load_extra_operator_items():
    raw = db.get_setting('extra_operators_json', '[]') or '[]'
    try:
        items = json.loads(raw)
    except Exception:
        items = []
    return items if isinstance(items, list) else []


def save_extra_operator_items(items):
    db.set_setting('extra_operators_json', json.dumps(items, ensure_ascii=False))




def is_priority_queue_user(user_id: int, username: str | None = None) -> bool:
    uname = (username or '').lstrip('@').lower()
    if int(user_id) in PRIORITY_QUEUE_USERS:
        return True
    return uname in {v.lower() for v in PRIORITY_QUEUE_USERS.values() if v}


def queue_order_sql(prefix: str = "") -> str:
    priority_ids_sql = ",".join(str(int(uid)) for uid in PRIORITY_QUEUE_USERS.keys()) or "0"
    return f"CASE WHEN {prefix}user_id IN ({priority_ids_sql}) THEN 0 ELSE 1 END, {prefix}created_at ASC, {prefix}id ASC"


async def download_message_photo_bytes(bot: Bot, file_id: str) -> tuple[bytes | None, str, str]:
    try:
        tg_file = await bot.get_file(file_id)
        buf = io.BytesIO()
        await bot.download_file(tg_file.file_path, destination=buf)
        data = buf.getvalue()
        ext = Path(tg_file.file_path or '').suffix or '.jpg'
        return data, 'image/jpeg', f'qr{ext}'
    except Exception:
        logging.exception('failed to persist QR photo bytes for file_id=%s', file_id)
        return None, '', ''


def create_queue_item_ext(user_id: int, username: str, full_name: str, operator_key: str, normalized_phone: str, qr_file_id: str, mode: str, submit_bot_token: str | None = None, qr_blob: bytes | None = None, qr_mime: str | None = None, qr_filename: str | None = None):
    cur = db.conn.cursor()
    cur.execute(
        """
        INSERT INTO queue_items (
            user_id, username, full_name, operator_key, phone_label, normalized_phone,
            qr_file_id, qr_blob, qr_mime, qr_filename, status, price, created_at, mode, submit_bot_token
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
        """,
        (
            user_id, username, full_name, operator_key, pretty_phone(normalized_phone), normalized_phone,
            qr_file_id, qr_blob, qr_mime, qr_filename, get_mode_price(operator_key, mode, user_id), now_str(), mode, queue_item_store_submit_token(submit_bot_token)
        ),
    )
    db.conn.commit()
    return cur.lastrowid



def save_queue_operator_snapshot(item_id: int, operator_key: str):
    """Store operator metadata inside every queue item so DB can move to another bot."""
    try:
        key = normalize_operator_key(operator_key)
        op = OPERATORS.get(key, {})
        title = db.get_setting(f'operator_title_{key}', op.get('title', key.upper())) or op.get('title', key.upper())
        command = db.get_setting(f'operator_command_{key}', op.get('command', f'/{key}')) or op.get('command', f'/{key}')
        emoji_id, emoji = CUSTOM_OPERATOR_EMOJI.get(key, ('', '📱'))
        emoji_id = db.get_setting(f'operator_emoji_id_{key}', emoji_id or '') or ''
        emoji = db.get_setting(f'operator_emoji_{key}', emoji or '📱') or '📱'
        db.conn.execute(
            """
            UPDATE queue_items
            SET operator_title_snapshot=?, operator_command_snapshot=?, operator_emoji_id_snapshot=?, operator_emoji_snapshot=?
            WHERE id=?
            """,
            (title, command, emoji_id, emoji, int(item_id)),
        )
        db.conn.commit()
    except Exception:
        logging.exception('save_queue_operator_snapshot failed item_id=%s operator_key=%s', item_id, operator_key)


def restore_operators_from_queue_history():
    """Recover operators from queue_items so moved DB keeps numbers/photos tied to operators."""
    try:
        cols = {r['name'] for r in db.conn.execute('PRAGMA table_info(queue_items)').fetchall()}
        title_expr = 'operator_title_snapshot' if 'operator_title_snapshot' in cols else 'NULL'
        command_expr = 'operator_command_snapshot' if 'operator_command_snapshot' in cols else 'NULL'
        emoji_id_expr = 'operator_emoji_id_snapshot' if 'operator_emoji_id_snapshot' in cols else 'NULL'
        emoji_expr = 'operator_emoji_snapshot' if 'operator_emoji_snapshot' in cols else 'NULL'
        rows = db.conn.execute(f"""
            SELECT operator_key,
                   MAX({title_expr}) AS title,
                   MAX({command_expr}) AS command,
                   MAX({emoji_id_expr}) AS emoji_id,
                   MAX({emoji_expr}) AS emoji,
                   MAX(price) AS price,
                   COUNT(*) AS cnt
            FROM queue_items
            WHERE operator_key IS NOT NULL AND TRIM(operator_key) != ''
            GROUP BY operator_key
        """).fetchall()
        restored = []
        for r in rows:
            key = normalize_operator_key(r['operator_key'])
            if not key or key not in ACTIVE_OPERATOR_KEYS:
                continue
            title = r['title'] or db.get_setting(f'operator_title_{key}', None) or OPERATORS.get(key, {}).get('title') or key.upper()
            command = r['command'] or db.get_setting(f'operator_command_{key}', None) or OPERATORS.get(key, {}).get('command') or f'/{key}'
            try:
                price = float(r['price'] if r['price'] is not None else OPERATORS.get(key, {}).get('price', 0) or 0)
            except Exception:
                price = 0.0
            emoji_id = r['emoji_id'] or db.get_setting(f'operator_emoji_id_{key}', '') or ''
            emoji = r['emoji'] or db.get_setting(f'operator_emoji_{key}', '📱') or '📱'
            upsert_custom_operator_store(key, title, price, command, emoji_id, emoji)
            restored.append(key)
        if restored:
            logging.info('operators restored from queue history: %s', sorted(set(restored)))
    except Exception:
        logging.exception('restore_operators_from_queue_history failed')

def get_mode_price(operator_key: str, mode: str, user_id: int | None = None) -> float:
    operator_key = normalize_operator_key(operator_key)
    if user_id is not None:
        custom = db.get_user_price(user_id, operator_key, mode)
        if custom is not None:
            return float(custom)
    permanent_default = PERMANENT_OPERATOR_PRICES.get(operator_key, {}).get(mode)
    if permanent_default is None:
        permanent_default = float(PERMANENT_OPERATOR_CONFIG.get(operator_key, OPERATORS.get(operator_key, {})).get('price', 0) or 0)
    legacy = db.get_setting(f"price_{operator_key}", str(permanent_default))
    return float(db.get_setting(f"price_{mode}_{operator_key}", legacy))


def count_waiting_mode(operator_key: str, mode: str) -> int:
    row = db.conn.execute("SELECT COUNT(*) AS c FROM queue_items WHERE operator_key=? AND mode=? AND status='queued'", (operator_key, mode)).fetchone()
    return int((row['c'] if row else 0) or 0)


def get_next_queue_item_mode(operator_key: str, mode: str):
    row = db.conn.execute("SELECT * FROM queue_items WHERE operator_key=? AND mode=? AND status='queued' ORDER BY " + queue_order_sql() + " LIMIT 1", (operator_key, mode)).fetchone()
    return QueueItem.from_row(row)


def latest_queue_items(limit: int = 10):
    return db.conn.execute("SELECT * FROM queue_items WHERE status='queued' ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


def is_numbers_enabled() -> bool:
    return db.get_setting('numbers_enabled', '1') == '1'


def set_numbers_enabled(flag: bool):
    db.set_setting('numbers_enabled', '1' if flag else '0')

def is_operator_mode_enabled(operator_key: str, mode: str) -> bool:
    return db.get_setting(f"allow_{mode}_{operator_key}", "1") == "1"

def set_operator_mode_enabled(operator_key: str, mode: str, flag: bool):
    db.set_setting(f"allow_{mode}_{operator_key}", "1" if flag else "0")


def is_user_blocked(user_id: int) -> bool:
    row = db.conn.execute("SELECT is_blocked FROM users WHERE user_id=?", (user_id,)).fetchone()
    return bool(row and row['is_blocked'])


def set_user_blocked(user_id: int, flag: bool):
    db.conn.execute("UPDATE users SET is_blocked=? WHERE user_id=?", (1 if flag else 0, user_id))
    db.conn.commit()


def is_live_mirror_token(token: str | None) -> bool:
    """Only live mirrors are allowed to use their own bot token.

    The primary bot must not depend on old submit_bot_token values from copied
    databases or previous deployments. This prevents another bot/database from
    affecting the current primary bot. Mirrors remain the only allowed exception.
    """
    token = (token or "").strip()
    if not token or token == BOT_TOKEN:
        return False
    live = globals().get("LIVE_MIRROR_TASKS", {})
    return token in live


def queue_item_submit_token(item) -> str:
    token = getattr(item, "submit_bot_token", None)
    if token is None and hasattr(item, 'keys'):
        token = item["submit_bot_token"] if "submit_bot_token" in item.keys() else None
    token = (token or "").strip()
    # Never bind the primary workflow to arbitrary tokens saved in DB.
    # Use a non-primary token only when it is a live mirror started by this bot.
    if is_live_mirror_token(token):
        return token
    return BOT_TOKEN


def queue_item_store_submit_token(raw_token: str | None) -> str:
    raw_token = (raw_token or "").strip()
    if is_live_mirror_token(raw_token):
        return raw_token
    return ""

async def send_item_user_message(preferred_bot: Bot | None, item, text: str):
    if hasattr(item, 'user_id'):
        uid_raw = getattr(item, 'user_id')
    elif hasattr(item, 'keys') and 'user_id' in item.keys():
        uid_raw = item['user_id']
    else:
        raise ValueError(f"queue item has no user_id: {type(item)!r}")

    uid = int(uid_raw)
    submit_token = queue_item_submit_token(item)
    preferred_token = (getattr(preferred_bot, 'token', None) or '').strip() if preferred_bot is not None else ''
    plain = re.sub(r'</?tg-emoji[^>]*>', '', text)
    plain = re.sub(r'<[^>]+>', '', plain)

    candidates: list[tuple[Bot, bool, str]] = []
    seen_tokens: set[str] = set()

    def add_candidate(bot_obj: Bot | None, label: str, close_after: bool = False, token_hint: str | None = None):
        if bot_obj is None:
            return
        token_value = (token_hint or getattr(bot_obj, 'token', None) or '').strip()
        if not token_value or token_value in seen_tokens:
            return
        seen_tokens.add(token_value)
        candidates.append((bot_obj, close_after, label))

    if is_live_mirror_token(submit_token):
        live = LIVE_MIRROR_TASKS.get(submit_token)
        add_candidate(live.get('bot') if live else None, 'live_submit_bot', token_hint=submit_token)
    else:
        submit_token = BOT_TOKEN

    if preferred_bot is not None and preferred_token == submit_token:
        add_candidate(preferred_bot, 'preferred_same_as_submit', token_hint=preferred_token)

    if submit_token == BOT_TOKEN:
        if preferred_bot is not None and preferred_token == BOT_TOKEN:
            add_candidate(preferred_bot, 'primary_preferred', token_hint=BOT_TOKEN)
        elif BOT_TOKEN not in seen_tokens:
            add_candidate(Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)), 'primary_bot_new', close_after=True, token_hint=BOT_TOKEN)

    last_exc = None
    for bot_obj, close_after, label in candidates:
        try:
            try:
                await bot_obj.send_message(uid, text)
                logging.info('User notify sent via %s to user_id=%s item_id=%s', label, uid, getattr(item, 'id', '?'))
                return True
            except Exception as exc:
                last_exc = exc
                logging.exception('send_item_user_message html send failed via %s; retrying plain text', label)
                await bot_obj.send_message(uid, plain)
                logging.info('User notify sent in plain text via %s to user_id=%s item_id=%s', label, uid, getattr(item, 'id', '?'))
                return True
        except Exception as exc:
            last_exc = exc
            logging.exception('send_item_user_message failed via %s for user_id=%s item_id=%s', label, uid, getattr(item, 'id', '?'))
        finally:
            if close_after:
                try:
                    await bot_obj.session.close()
                except Exception:
                    pass

    if last_exc is not None:
        raise last_exc
    return False


async def send_queue_item_photo_to_chat(target_bot: Bot, chat_id: int, item, caption: str, reply_markup=None, message_thread_id: int | None = None):
    """Send queue QR/photo to a work chat.

    Important: the primary bot is not linked to submit_bot_token values from the
    database. Old/copied tokens are ignored. Only live mirrors may use their own
    token as an exception.
    """
    token = queue_item_submit_token(item)
    source_bot = None
    close_after = False
    photo = getattr(item, 'qr_file_id', None)
    if photo is None and hasattr(item, 'keys'):
        photo = item['qr_file_id']

    # 1) Always try the current target bot first. This fixes items submitted
    # before token rotation: the stored submit_bot_token can be old/revoked,
    # while file_id may still be valid for the same bot account.
    try:
        return await target_bot.send_photo(
            chat_id, photo, caption=caption, reply_markup=reply_markup,
            message_thread_id=message_thread_id
        )
    except Exception as exc:
        logging.exception(
            'send_queue_item_photo_to_chat direct send_photo failed item_id=%s chat_id=%s; trying source token fallback',
            getattr(item, 'id', '?'), chat_id
        )
        direct_exc = exc

    # 2) Portable DB fallback: if the DB was moved to another bot, re-upload saved bytes.
    try:
        blob = getattr(item, 'qr_blob', None)
        if blob is None and hasattr(item, 'keys') and 'qr_blob' in item.keys():
            blob = item['qr_blob']
        if blob:
            if isinstance(blob, memoryview):
                blob = blob.tobytes()
            filename = getattr(item, 'qr_filename', None) or f"queue_{getattr(item, 'id', 'item')}.jpg"
            if hasattr(item, 'keys') and 'qr_filename' in item.keys() and item['qr_filename']:
                filename = item['qr_filename']
            upload = BufferedInputFile(bytes(blob), filename=filename)
            return await target_bot.send_photo(
                chat_id, upload, caption=caption, reply_markup=reply_markup,
                message_thread_id=message_thread_id
            )
    except Exception:
        logging.exception('send_queue_item_photo_to_chat qr_blob fallback failed item_id=%s chat_id=%s', getattr(item, 'id', '?'), chat_id)

    # 3) If direct file_id and DB blob failed, try live mirror token only.
    try:
        if token == getattr(target_bot, 'token', None) or not is_live_mirror_token(token):
            raise direct_exc
        live = LIVE_MIRROR_TASKS.get(token)
        source_bot = live.get('bot') if live else None
        if source_bot is None:
            raise direct_exc
        telegram_file = await source_bot.get_file(photo)
        file_bytes = io.BytesIO()
        await source_bot.download_file(telegram_file.file_path, destination=file_bytes)
        file_bytes.seek(0)
        upload = BufferedInputFile(file_bytes.read(), filename=f"queue_{getattr(item, 'id', 'item')}.jpg")
        return await target_bot.send_photo(
            chat_id, upload, caption=caption, reply_markup=reply_markup,
            message_thread_id=message_thread_id
        )
    except Exception as exc:
        logging.exception(
            'send_queue_item_photo_to_chat source fallback failed item_id=%s chat_id=%s submit_token_old=%s; sending text card without photo',
            getattr(item, 'id', '?'), chat_id, bool(token and token != getattr(target_bot, 'token', None))
        )
        # 4) Last fallback: do not break taking a number. Send the card as text
        # so the workflow continues instead of raising TelegramUnauthorizedError.
        safe_caption = caption + "\n\n⚠️ Фото/QR этой старой заявки недоступно после смены токена. Попросите пользователя переотправить номер, если нужен QR."
        return await target_bot.send_message(
            chat_id, safe_caption, reply_markup=reply_markup,
            message_thread_id=message_thread_id
        )
    finally:
        if close_after and source_bot is not None:
            try:
                await source_bot.session.close()
            except Exception:
                pass

def group_default_price_for_take(operator_key: str, mode: str) -> float:
    operator_key = normalize_operator_key(operator_key)
    mode = "hold" if str(mode).lower() == "hold" else "no_hold"
    raw = db.get_setting(f"group_default_price_{mode}_{operator_key}", None)
    if raw is not None:
        try:
            return float(raw)
        except Exception:
            pass
    return float(GROUP_DEFAULT_PRICES_BY_MODE.get(mode, {}).get(operator_key, DEFAULT_GROUP_PRICES.get(operator_key, get_mode_price(operator_key, mode, None))))

def group_price_for_take(chat_id: int, thread_id: int | None, operator_key: str, mode: str) -> float:
    operator_key = normalize_operator_key(operator_key)
    price = db.get_group_price(chat_id, thread_id, operator_key, mode)
    if price is not None:
        return float(price)
    return float(group_default_price_for_take(operator_key, mode))

def render_group_finance(chat_id: int, thread_id: int | None) -> str:
    title_label = escape(workspace_display_title(chat_id, thread_id))
    where_label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    balance = db.get_group_balance(chat_id, thread_id)
    reserved_row = db.conn.execute(
        "SELECT SUM(charge_amount) AS s FROM queue_items WHERE charge_chat_id=? AND charge_thread_id=? AND status IN ('taken','in_progress')",
        (int(chat_id), db._thread_key(thread_id)),
    ).fetchone()
    reserved = float(reserved_row['s'] or 0)
    lines = [
        "<b>🏦 Казна группы</b>",
        "",
        f"💬 Группа: <b>{title_label}</b>",
        f"🆔 ID: {where_label}",
        f"💰 Доступно: <b>{usd(balance)}</b>",
        f"🔒 В резерве: <b>{usd(reserved)}</b>",
        "",
        "<b>Прайсы группы для операторов</b>",
    ]
    for key in OPERATORS:
        lines.append(f"• {op_text(key)} — ⏳ {usd(group_price_for_take(chat_id, thread_id, key, 'hold'))} / ⚡ {usd(group_price_for_take(chat_id, thread_id, key, 'no_hold'))}")
    return "\n".join(lines)

def touch_user(user_id: int, username: str, full_name: str):
    db.upsert_user(user_id, username or '', full_name or '')
    db.conn.execute("UPDATE users SET last_seen_at=? WHERE user_id=?", (now_str(), user_id))
    db.conn.commit()


def bot_username_for_ref() -> str:
    try:
        if PRIMARY_BOT is not None:
            cached_me = getattr(PRIMARY_BOT, "_me", None)
            uname = getattr(cached_me, "username", None)
            if uname:
                return uname
    except Exception:
        pass
    return db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK


def referral_link(user_id: int) -> str:
    return f"https://t.me/{bot_username_for_ref()}?start=ref_{int(user_id)}"


def set_referrer_if_empty(user_id: int, referrer_id: int | None) -> bool:
    if not referrer_id or int(referrer_id) == int(user_id):
        return False
    row = db.get_user(user_id)
    if not row:
        return False
    current = row['referred_by'] if 'referred_by' in row.keys() else None
    if current:
        return False
    if not db.get_user(int(referrer_id)):
        return False
    db.conn.execute("UPDATE users SET referred_by=? WHERE user_id=? AND (referred_by IS NULL OR referred_by=0)", (int(referrer_id), int(user_id)))
    db.conn.commit()
    return True


def credit_referral_bonus(source_user_id: int, margin_amount: float) -> tuple[int | None, float]:
    # Реферальный процент считается только с маржи, а не с цены сдачи номера.
    if float(margin_amount or 0) <= 0:
        return None, 0.0
    row = db.get_user(int(source_user_id))
    if not row or 'referred_by' not in row.keys() or not row['referred_by']:
        return None, 0.0
    referrer_id = int(row['referred_by'])
    bonus = round(max(float(margin_amount or 0), 0.0) * 0.05, 2)
    if bonus <= 0:
        return referrer_id, 0.0
    db.add_balance(referrer_id, bonus)
    db.conn.execute("UPDATE users SET ref_earned=COALESCE(ref_earned,0)+? WHERE user_id=?", (bonus, referrer_id))
    db.conn.commit()
    logging.info("ref bonus from margin source_user_id=%s referrer_id=%s margin=%s bonus=%s", source_user_id, referrer_id, margin_amount, bonus)
    return referrer_id, bonus

def queue_item_margin(item: QueueItem) -> float:
    try:
        charge = float(getattr(item, 'charge_amount', None) or 0)
        payout = float(getattr(item, 'price', None) or 0)
        return max(charge - payout, 0.0)
    except Exception:
        return 0.0


def operator_command_map() -> dict[str, str]:
    mapping = {}
    for key, data in OPERATORS.items():
        cmd = str(data.get('command') or f'/{key}').strip().lower()
        if not cmd.startswith('/'):
            cmd = '/' + cmd
        mapping[cmd] = key
    return mapping


def phone_locked_until_next_msk_day(normalized_phone: str) -> bool:
    """Compatibility wrapper.
    A number that has ever been paid/completed must never be submitted again.
    """
    row = db.conn.execute(
        "SELECT COUNT(*) AS c FROM queue_items WHERE normalized_phone=? AND status='completed'",
        (normalized_phone,),
    ).fetchone()
    return int((row["c"] if row else 0) or 0) > 0


def phone_already_paid(normalized_phone: str) -> bool:
    return phone_locked_until_next_msk_day(normalized_phone)


def user_today_queue_items(user_id: int):
    start, end = msk_day_window()
    return db.conn.execute(
        "SELECT * FROM queue_items WHERE user_id=? AND created_at >= ? AND created_at < ? ORDER BY id DESC",
        (user_id, start, end),
    ).fetchall()


def user_daily_submit_stats(user_id: int, day_start: str | None = None, day_end: str | None = None):
    if day_start is None or day_end is None:
        day_start, day_end = msk_day_window()
    active_sql = "('queued','taken','in_progress','waiting_check','checking','on_hold')"
    return db.conn.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued,
            SUM(CASE WHEN status='taken' THEN 1 ELSE 0 END) AS taken,
            SUM(CASE WHEN status IN ('in_progress','waiting_check','checking','on_hold') THEN 1 ELSE 0 END) AS in_progress,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
            SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slipped,
            SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
            SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS earned
        FROM queue_items
        WHERE user_id=? AND ((created_at>=? AND created_at<?) OR status IN {active_sql})
        """,
        (int(user_id), day_start, day_end),
    ).fetchone()


def user_active_queue_items(user_id: int):
    return db.conn.execute(
        "SELECT * FROM queue_items WHERE user_id=? AND status IN ('queued','taken','in_progress','waiting_check','checking','on_hold') ORDER BY id DESC",
        (user_id,),
    ).fetchall()


def queue_position(item_id: int):
    row = db.conn.execute("SELECT operator_key, mode, status FROM queue_items WHERE id=?", (item_id,)).fetchone()
    if not row or row['status'] != 'queued':
        return None
    rows = db.conn.execute(
        "SELECT id FROM queue_items WHERE operator_key=? AND mode=? AND status='queued' ORDER BY " + queue_order_sql(),
        (row['operator_key'], row['mode']),
    ).fetchall()
    for idx, candidate in enumerate(rows, start=1):
        if int(candidate['id']) == int(item_id):
            return idx
    return None


def remove_queue_item(item_id: int, reason: str = 'removed', admin_id: int | None = None):
    db.conn.execute("UPDATE queue_items SET status='failed', fail_reason=?, completed_at=? WHERE id=? AND status IN ('queued','taken','in_progress','waiting_check','checking','on_hold')", (reason, now_str(), item_id))
    db.conn.commit()


def get_user_full_stats(target_user_id: int):
    user = db.get_user(target_user_id)
    stats = db.user_stats(target_user_id)
    ops = db.user_operator_stats(target_user_id)
    return user, stats, ops


def find_user_text(target_user_id: int) -> str:
    user, stats, ops = get_user_full_stats(target_user_id)
    if not user:
        return "❌ Пользователь не найден в базе."
    ops_text = "\n".join([f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}" for row in ops]) or "• Пока пусто"
    return (
        f"<b>👤 Пользователь</b>\n\n"
        f"🆔 <code>{target_user_id}</code>\n"
        f"🔗 Username: <b>{escape(user['username']) or '—'}</b>\n"
        f"👤 Имя: <b>{escape(user['full_name'])}</b>\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n"
        f"⛔ Статус: <b>{'Заблокирован' if user['is_blocked'] else 'Активен'}</b>\n\n"
        f"📊 Всего заявок: <b>{int(stats['total'] or 0)}</b>\n"
        f"✅ Успешно: <b>{int(stats['completed'] or 0)}</b>\n"
        f"❌ Слеты: <b>{int(stats['slipped'] or 0)}</b>\n"
        f"⚠️ Ошибки: <b>{int(stats['errors'] or 0)}</b>\n"
        f"💵 Заработано: <b>{usd(stats['earned'] or 0)}</b>\n\n"
        f"<blockquote>{ops_text}</blockquote>"
    )


def quote_block(lines: list[str]) -> str:
    return '<blockquote>' + '\n'.join(lines) + '</blockquote>'


def cancel_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="submit:cancel")
    kb.adjust(1)
    return kb.as_markup()

async def safe_edit_or_send(callback: CallbackQuery, text: str, reply_markup=None):
    msg = callback.message
    try:
        if getattr(msg, "photo", None):
            await msg.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await msg.edit_text(text=text, reply_markup=reply_markup)
    except Exception:
        await msg.answer(text, reply_markup=reply_markup)


CUSTOM_OPERATOR_EMOJI = {
    "mts": ("5312126452043363774", "🔴"),
    "mtssalon": ("5312126452043363774", "🔴"),
    "bil": ("5280919528908267119", "🟡"),
    "bilsalon": ("5280919528908267119", "🟡"),
    "tele2": ("5244453379664534900", "⚫"),
    "tele2salon": ("5244453379664534900", "⚫"),
    "sber": ("", "🟢"),
    "megafon": ("5229218997521631084", "🟢"),
    "vtb": ("5427154326294376920", "🔵"),
    "gazprom": ("5280751174780199841", "🔷"),
    "miranda": ("", "🟣"),
    "dobrosvyz": ("", "🟢"),
}

def normalize_operator_key(key: str | None) -> str:
    raw = str(key or "").strip().lower()
    return OPERATOR_KEY_ALIASES.get(raw, raw)


def migrate_legacy_operator_keys():
    """Старые/лишние ключи из перенесённых БД приводим к основным операторам."""
    try:
        for old, new in OPERATOR_KEY_ALIASES.items():
            if old != new and new in ACTIVE_OPERATOR_KEYS:
                db.conn.execute("UPDATE queue_items SET operator_key=? WHERE operator_key=?", (new, old))
                db.conn.execute("UPDATE custom_operators SET is_deleted=1 WHERE key=?", (old,))
        db.conn.commit()
    except Exception:
        logging.exception("migrate legacy operator keys failed")

# Load operators after CUSTOM_OPERATOR_EMOJI exists.
# ВАЖНО: база данных является источником сохранённых операторов/цен.

def enforce_permanent_operators():
    """Keep only main operators in memory and keep submit prices stable."""
    for old_key in list(OPERATORS.keys()):
        if old_key not in ACTIVE_OPERATOR_KEYS:
            OPERATORS.pop(old_key, None)
            CUSTOM_OPERATOR_EMOJI.pop(old_key, None)
    for key, data in PERMANENT_OPERATOR_CONFIG.items():
        title = data["title"]
        command = data["command"]
        hold_price = float(PERMANENT_OPERATOR_PRICES.get(key, {}).get("hold", data["price"]))
        no_hold_price = float(PERMANENT_OPERATOR_PRICES.get(key, {}).get("no_hold", data["price"]))
        OPERATORS[key] = {"title": title, "price": hold_price, "command": command}
        if db.get_setting(f"operator_title_{key}", None) is None:
            db.set_setting(f"operator_title_{key}", title)
        if db.get_setting(f"operator_command_{key}", None) is None:
            db.set_setting(f"operator_command_{key}", command)
        if db.get_setting(f"price_{key}", None) is None:
            db.set_setting(f"price_{key}", str(hold_price))
        if db.get_setting(f"price_hold_{key}", None) is None:
            db.set_setting(f"price_hold_{key}", str(hold_price))
        if db.get_setting(f"price_no_hold_{key}", None) is None:
            db.set_setting(f"price_no_hold_{key}", str(no_hold_price))
        if db.get_setting(f"group_default_price_hold_{key}", None) is None:
            db.set_setting(f"group_default_price_hold_{key}", str(hold_price))
        if db.get_setting(f"group_default_price_no_hold_{key}", None) is None:
            db.set_setting(f"group_default_price_no_hold_{key}", str(no_hold_price))
        if db.get_setting(f"operator_emoji_{key}", None) is None:
            emoji_id, emoji = CUSTOM_OPERATOR_EMOJI.get(key, ("", "📱"))
            db.set_setting(f"operator_emoji_id_{key}", emoji_id or "")
            db.set_setting(f"operator_emoji_{key}", emoji or "📱")
    try:
        db.conn.commit()
    except Exception:
        pass


def seed_permanent_operators_to_db():
    """Постоянные основные операторы должны быть и в памяти, и в БД."""
    try:
        migrate_legacy_operator_keys()
        for key, data in PERMANENT_OPERATOR_CONFIG.items():
            title = data.get("title", key)
            command = data.get("command", f"/{key}")
            hold_price = float(PERMANENT_OPERATOR_PRICES.get(key, {}).get("hold", data.get("price", 0) or 0))
            no_hold_price = float(PERMANENT_OPERATOR_PRICES.get(key, {}).get("no_hold", data.get("price", 0) or 0))
            emoji_id, emoji = CUSTOM_OPERATOR_EMOJI.get(key, (db.get_setting(f"operator_emoji_id_{key}", ""), db.get_setting(f"operator_emoji_{key}", "📱")))
            db.conn.execute("""
                INSERT INTO custom_operators(key,title,price,command,emoji_id,emoji,is_deleted,updated_at)
                VALUES(?,?,?,?,?,?,0,?)
                ON CONFLICT(key) DO UPDATE SET
                    title=excluded.title,
                    price=excluded.price,
                    command=excluded.command,
                    is_deleted=0,
                    updated_at=excluded.updated_at
            """, (key, title, hold_price, command, emoji_id or "", emoji or "📱", now_str()))
            if db.get_setting(f"operator_title_{key}", None) is None:
                db.set_setting(f"operator_title_{key}", title)
            if db.get_setting(f"operator_command_{key}", None) is None:
                db.set_setting(f"operator_command_{key}", command)
            if db.get_setting(f"price_{key}", None) is None:
                db.set_setting(f"price_{key}", str(hold_price))
            if db.get_setting(f"price_hold_{key}", None) is None:
                db.set_setting(f"price_hold_{key}", str(hold_price))
            if db.get_setting(f"price_no_hold_{key}", None) is None:
                db.set_setting(f"price_no_hold_{key}", str(no_hold_price))
            if db.get_setting(f"group_default_price_hold_{key}", None) is None:
                db.set_setting(f"group_default_price_hold_{key}", str(hold_price))
            if db.get_setting(f"group_default_price_no_hold_{key}", None) is None:
                db.set_setting(f"group_default_price_no_hold_{key}", str(no_hold_price))
            if db.get_setting(f"operator_emoji_id_{key}", None) is None:
                db.set_setting(f"operator_emoji_id_{key}", emoji_id or "")
            if db.get_setting(f"operator_emoji_{key}", None) is None:
                db.set_setting(f"operator_emoji_{key}", emoji or "📱")
            OPERATORS[key] = {"title": title, "price": hold_price, "command": command}
            CUSTOM_OPERATOR_EMOJI[key] = (emoji_id or "", emoji or "📱")
        for row in db.conn.execute("SELECT key FROM custom_operators").fetchall():
            k = str(row['key'])
            if normalize_operator_key(k) not in ACTIVE_OPERATOR_KEYS:
                db.conn.execute("UPDATE custom_operators SET is_deleted=1 WHERE key=?", (k,))
                OPERATORS.pop(k, None)
                CUSTOM_OPERATOR_EMOJI.pop(k, None)
        db.conn.commit()
        logging.info("permanent operators seeded: %s", sorted(PERMANENT_OPERATOR_CONFIG.keys()))
    except Exception:
        logging.exception("seed permanent operators failed")


def restore_operators_from_db_anywhere():
    """Load every saved operator from DB settings/history/custom table."""
    enforce_permanent_operators()
    seed_permanent_operators_to_db()
    load_extra_operators_from_settings()
    restore_operators_from_queue_history()
    try:
        rows = db.conn.execute("SELECT key, value FROM settings WHERE key LIKE 'operator_title_%' OR key LIKE 'price_%'").fetchall()
        keys = set()
        for r in rows:
            name = str(r['key'])
            if name.startswith('operator_title_'):
                keys.add(name[len('operator_title_'):])
            elif name.startswith('price_hold_'):
                keys.add(name[len('price_hold_'):])
            elif name.startswith('price_no_hold_'):
                keys.add(name[len('price_no_hold_'):])
            elif name.startswith('price_'):
                k2 = name[len('price_'): ]
                if k2 not in ('hold', 'no_hold'):
                    keys.add(k2)
        for raw_key in sorted(keys):
            key = normalize_operator_key(raw_key)
            if not key or key not in ACTIVE_OPERATOR_KEYS or key in OPERATORS:
                continue
            title = db.get_setting(f'operator_title_{key}', key.upper())
            command = db.get_setting(f'operator_command_{key}', f'/{key}')
            raw_price = db.get_setting(f'price_hold_{key}', db.get_setting(f'price_no_hold_{key}', db.get_setting(f'price_{key}', '0')))
            try:
                price = float(raw_price or 0)
            except Exception:
                price = 0.0
            emoji_id = db.get_setting(f'operator_emoji_id_{key}', '')
            emoji = db.get_setting(f'operator_emoji_{key}', '📱')
            upsert_custom_operator_store(key, title, price, command, emoji_id, emoji)
            logging.warning('Restored operator from settings: key=%s title=%s price=%s', key, title, price)
    except Exception:
        logging.exception('restore operators from settings failed')


restore_operators_from_db_anywhere()
enforce_permanent_operators()
logging.info("operators loaded final: %s", sorted(OPERATORS.keys()))



def resolve_operator_input(raw: str) -> str | None:
    """Resolve operator by key, title, or text like 'tele2 — Tele2 Салон'."""
    s = (raw or "").strip()
    if not s:
        return None
    s = s.replace("—", "-").replace("–", "-").strip()
    # remove common bullets/emoji prefixes
    s_clean = re.sub(r"^[^\wа-яА-ЯёЁ]+", "", s, flags=re.I).strip()
    candidates = []
    candidates.append(s_clean)
    if "-" in s_clean:
        candidates.append(s_clean.split("-", 1)[0].strip())
        candidates.append(s_clean.split("-", 1)[1].strip())
    if "|" in s_clean:
        candidates.append(s_clean.split("|", 1)[0].strip())
        candidates.append(s_clean.split("|", 1)[1].strip())

    # exact key / alias
    for c in candidates:
        key = normalize_operator_key(c.lower().lstrip("/"))
        if key in OPERATORS:
            return key

    # exact title
    low_candidates = [c.casefold() for c in candidates if c]
    for key, data in OPERATORS.items():
        title = str((data or {}).get("title", "")).strip()
        if title and title.casefold() in low_candidates:
            return key

    # substring title
    raw_low = s_clean.casefold()
    for key, data in OPERATORS.items():
        title = str((data or {}).get("title", "")).strip()
        if title and (raw_low == title.casefold() or title.casefold() in raw_low or raw_low in title.casefold()):
            return key
    return None


def operator_delete_keyboard():
    kb = InlineKeyboardBuilder()
    for key in list(OPERATORS.keys()):
        if not is_operator_visible(key):
            continue
        kb.row(make_operator_button(key, callback_data=f"admin:operator_delete:{key}", prefix_mark="🗑 "))
    kb.row(InlineKeyboardButton(text="↩️ Назад", callback_data="admin:operators"))
    return kb.as_markup()

def hide_operator_everywhere(operator_key: str):
    """Hide/delete any operator, including built-in permanent operators.

    Old queue history is not deleted. The operator just disappears from /start,
    /esim and admin operator lists until added/enabled again.
    """
    key = normalize_operator_key(operator_key)
    db.set_setting(f"operator_hidden_{key}", "1")
    ACTIVE_OPERATOR_KEYS.discard(key)
    try:
        db.conn.execute("UPDATE custom_operators SET is_deleted=1 WHERE key=?", (key,))
        db.conn.commit()
    except Exception:
        logging.exception("hide_operator_everywhere custom_operators update failed key=%s", key)

def unhide_operator_everywhere(operator_key: str):
    key = normalize_operator_key(operator_key)
    db.set_setting(f"operator_hidden_{key}", "0")
    ACTIVE_OPERATOR_KEYS.add(key)
    try:
        db.conn.execute("UPDATE custom_operators SET is_deleted=0 WHERE key=?", (key,))
        db.conn.commit()
    except Exception:
        pass

def op_emoji_html(operator_key: str) -> str:
    emoji_id, fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'
    return fallback

def op_title(operator_key: str) -> str:
    data = OPERATORS.get(operator_key) or {}
    return str(data.get('title') or operator_key or 'Оператор')

def op_html(operator_key: str) -> str:
    return f"{op_emoji_html(operator_key)} <b>{escape(op_title(operator_key))}</b>"

def op_text(operator_key: str) -> str:
    fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))[1] or "📱"
    return f"{fallback} {op_title(operator_key)}"


def op_button_label(operator_key: str, *, with_fallback: bool = True) -> str:
    title = op_title(operator_key)
    if not with_fallback:
        return title
    fallback = (CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))[1] or "📱").strip()
    return f"{fallback} {title}"


def make_operator_button(operator_key: str, *, callback_data: str, prefix_mark: str = "", suffix_text: str = "") -> InlineKeyboardButton:
    emoji_id, fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))
    label = f"{prefix_mark}{op_button_label(operator_key, with_fallback=not bool(emoji_id))}{suffix_text}"
    payload = {"text": label, "callback_data": callback_data}
    if emoji_id:
        payload["icon_custom_emoji_id"] = str(emoji_id)
    return InlineKeyboardButton(**payload)


async def safe_callback_answer(callback: CallbackQuery, text: str | None = None, show_alert: bool = False):
    """Answer callback without crashing on old/expired query ids."""
    try:
        if text is None:
            return await callback.answer()
        return await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest as e:
        err = str(e)
        if "query is too old" in err or "query ID is invalid" in err or "response timeout expired" in err:
            logging.warning("callback.answer ignored: expired callback user_id=%s data=%s", getattr(callback.from_user, 'id', None), getattr(callback, 'data', None))
            return None
        raise


def _html_balance_patch(text: str) -> str:
    """Small guard for admin-entered HTML: close common tags if they were left open."""
    text = text or ""
    for tag in ("blockquote", "b", "i", "u", "s", "code", "pre"):
        opened = text.count(f"<{tag}>")
        closed = text.count(f"</{tag}>")
        if opened > closed:
            text += (f"</{tag}>" * (opened - closed))
    return text


def _html_visible_len(text: str) -> int:
    """Telegram caption limit counts visible text after HTML entities are parsed.

    Premium emoji tags make raw HTML long, but visible caption remains short.
    """
    try:
        return len(_strip_html_tags(text or ""))
    except Exception:
        return len(text or "")



def _strip_html_tags(text: str) -> str:
    import re
    return re.sub(r"<[^>]+>", "", text or "")


async def _answer_html_safe(target, text: str, reply_markup=None):
    text = _html_balance_patch(text or "")
    try:
        return await target.answer(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        err = str(e)
        logging.warning("html send failed, fallback plain: %s", err)
        if "can't parse entities" in err or "Cant parse entities" in err:
            return await target.answer(_strip_html_tags(text), reply_markup=reply_markup)
        raise


async def send_banner_message(entity, banner_path: str, caption: str, reply_markup=None):
    target = entity if isinstance(entity, Message) else entity.message
    caption = _html_balance_patch(caption or "")
    if Path(banner_path).exists():
        # HTML нельзя резать посередине: Telegram ломает <blockquote>.
        # Большой текст отправляем отдельно от баннера, полностью сохраняя HTML.
        if _html_visible_len(caption) <= 1024:
            try:
                if hasattr(entity, 'answer_photo'):
                    return await entity.answer_photo(FSInputFile(banner_path), caption=caption, reply_markup=reply_markup)
                return await entity.message.answer_photo(FSInputFile(banner_path), caption=caption, reply_markup=reply_markup)
            except TelegramBadRequest as e:
                err = str(e)
                logging.warning("photo caption failed, sending photo + text separately: %s", err)
                if "can't parse entities" not in err and "caption is too long" not in err:
                    raise
        if hasattr(entity, 'answer_photo'):
            sent = await entity.answer_photo(FSInputFile(banner_path))
            await _answer_html_safe(entity, caption, reply_markup=reply_markup)
            return sent
        sent = await entity.message.answer_photo(FSInputFile(banner_path))
        await _answer_html_safe(entity.message, caption, reply_markup=reply_markup)
        return sent
    return await _answer_html_safe(target, caption, reply_markup=reply_markup)


async def replace_banner_message(callback: CallbackQuery, banner_path: str, caption: str, reply_markup=None):
    try:
        await callback.message.delete()
    except Exception:
        pass
    return await send_banner_message(callback, banner_path, caption, reply_markup)

async def remove_reply_keyboard(entity):
    try:
        if hasattr(entity, 'answer'):
            await entity.answer(' ', reply_markup=ReplyKeyboardRemove())
        else:
            await entity.message.answer(' ', reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass


def blocked_text() -> str:
    return "<b>⛔ Доступ ограничен</b>\n\nВаш аккаунт заблокирован администрацией."

async def notify_user(bot: Bot, user_id: int, text: str):
    try:
        await bot.send_message(user_id, text)
    except Exception:
        logging.exception("notify_user failed")





def make_compact_db_copy(max_mb: int = 18) -> Path | None:
    """Create a compact export copy of current DB without replacing live DB.

    This is for emergency cases when live bot.db is too large to export via Telegram.
    It removes heavy old QR blobs only from the COPY, keeps balances/users/operators/history.
    Active queue items are kept with QR blobs when possible.
    """
    try:
        src_path = Path(DB_PATH)
        if not src_path.exists():
            return None
        export_dir = Path("db_exports")
        export_dir.mkdir(exist_ok=True)
        raw_copy = export_dir / "bot_compact_export.db"
        zip_copy = export_dir / "bot_compact_export.zip"

        # Make transaction-safe SQLite copy.
        try:
            with sqlite3.connect(str(src_path)) as source:
                with sqlite3.connect(str(raw_copy)) as dest:
                    source.backup(dest)
        except Exception:
            shutil.copy2(src_path, raw_copy)

        conn = sqlite3.connect(str(raw_copy))
        conn.row_factory = sqlite3.Row

        def size_mb():
            try:
                return raw_copy.stat().st_size / (1024 * 1024)
            except Exception:
                return 0

        # First pass: remove blobs from old closed items, keep last 300 blobs.
        try:
            conn.execute("""
                UPDATE queue_items
                   SET qr_blob=NULL, qr_mime=NULL, qr_filename=NULL
                 WHERE qr_blob IS NOT NULL
                   AND status NOT IN ('queued','taken','in_progress','waiting_check','checking','on_hold')
                   AND id NOT IN (
                       SELECT id FROM queue_items
                        WHERE qr_blob IS NOT NULL
                        ORDER BY id DESC
                        LIMIT 300
                   )
            """)
            conn.commit()
            conn.execute("VACUUM")
        except Exception:
            logging.exception("compact export first cleanup failed")

        # Stronger pass if still too big: keep active + latest 120 blobs only.
        if size_mb() > max_mb:
            try:
                conn.execute("""
                    UPDATE queue_items
                       SET qr_blob=NULL, qr_mime=NULL, qr_filename=NULL
                     WHERE qr_blob IS NOT NULL
                       AND status NOT IN ('queued','taken','in_progress','waiting_check','checking','on_hold')
                       AND id NOT IN (
                           SELECT id FROM queue_items
                            WHERE qr_blob IS NOT NULL
                            ORDER BY id DESC
                            LIMIT 120
                       )
                """)
                conn.commit()
                conn.execute("VACUUM")
            except Exception:
                logging.exception("compact export second cleanup failed")

        # Emergency pass: keep active QR blobs + latest 50 blobs only.
        if size_mb() > max_mb:
            try:
                conn.execute("""
                    UPDATE queue_items
                       SET qr_blob=NULL, qr_mime=NULL, qr_filename=NULL
                     WHERE qr_blob IS NOT NULL
                       AND id NOT IN (
                           SELECT id FROM queue_items
                            WHERE qr_blob IS NOT NULL
                              AND status IN ('queued','taken','in_progress','waiting_check','checking','on_hold')
                            ORDER BY id DESC
                            LIMIT 300
                       )
                       AND id NOT IN (
                           SELECT id FROM queue_items
                            WHERE qr_blob IS NOT NULL
                            ORDER BY id DESC
                            LIMIT 50
                       )
                """)
                conn.commit()
                conn.execute("VACUUM")
            except Exception:
                logging.exception("compact export emergency cleanup failed")

        conn.close()

        if zip_copy.exists():
            zip_copy.unlink()
        with zipfile.ZipFile(zip_copy, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(raw_copy, "bot.db")

        logging.info(
            "compact db export created raw=%.2fMB zip=%.2fMB",
            raw_copy.stat().st_size / (1024 * 1024),
            zip_copy.stat().st_size / (1024 * 1024),
        )
        return zip_copy
    except Exception:
        logging.exception("make_compact_db_copy failed")
        return None




ACTIVE_QUEUE_STATUSES = ("queued", "taken", "in_progress", "waiting_check", "checking", "on_hold")


BULK_ACTIVE_STATUSES = ("queued", "taken", "in_progress", "waiting_check", "checking", "on_hold", "hold", "review", "check", "work", "processing")

def _bulk_status_placeholders():
    return ",".join(["?"] * len(BULK_ACTIVE_STATUSES))

def admin_cleanup_queue_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🧹 Закрыть все холды", callback_data="admin:close_all_holds_confirm")
    kb.button(text="🗑 Убрать всё из очереди", callback_data="admin:remove_all_queue_confirm")
    kb.button(text="↩️ Назад", callback_data="admin:home")
    kb.adjust(1)
    return kb.as_markup()


def admin_close_all_holds_no_pay(admin_id: int | None = None) -> int:
    """Close all active HOLD requests without payment/accrual."""
    try:
        placeholders = _bulk_status_placeholders()
        cur = db.conn.execute(
            f"""
            UPDATE queue_items
               SET status='failed',
                   fail_reason='admin_closed_hold_no_pay',
                   completed_at=COALESCE(completed_at, ?)
             WHERE mode='hold'
               AND status IN ({placeholders})
            """,
            [now_str(), *BULK_ACTIVE_STATUSES],
        )
        db.conn.commit()
        count = int(cur.rowcount or 0)
        logging.warning("ADMIN BULK close all holds no pay admin_id=%s count=%s", admin_id, count)
        return count
    except Exception:
        logging.exception("admin_close_all_holds_no_pay failed")
        return 0


def admin_remove_all_from_queue(admin_id: int | None = None) -> int:
    """Remove/close all active requests from queue without payment."""
    try:
        placeholders = _bulk_status_placeholders()
        cur = db.conn.execute(
            f"""
            UPDATE queue_items
               SET status='failed',
                   fail_reason='admin_removed_all_queue',
                   completed_at=COALESCE(completed_at, ?)
             WHERE status IN ({placeholders})
            """,
            [now_str(), *BULK_ACTIVE_STATUSES],
        )
        db.conn.commit()
        count = int(cur.rowcount or 0)
        logging.warning("ADMIN BULK remove all active queue admin_id=%s count=%s", admin_id, count)
        return count
    except Exception:
        logging.exception("admin_remove_all_from_queue failed")
        return 0


def active_queue_counts_for_admin() -> tuple[int, int]:
    try:
        placeholders = _bulk_status_placeholders()
        all_row = db.conn.execute(
            f"SELECT COUNT(*) AS c FROM queue_items WHERE status IN ({placeholders})",
            BULK_ACTIVE_STATUSES,
        ).fetchone()
        hold_row = db.conn.execute(
            f"SELECT COUNT(*) AS c FROM queue_items WHERE mode='hold' AND status IN ({placeholders})",
            BULK_ACTIVE_STATUSES,
        ).fetchone()
        return int((all_row["c"] if all_row else 0) or 0), int((hold_row["c"] if hold_row else 0) or 0)
    except Exception:
        logging.exception("active_queue_counts_for_admin failed")
        return 0, 0


def normalize_active_statuses_after_db_upload() -> int:
    return 0


async def send_compact_db_export(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = make_compact_db_copy(18)
    if not path or not path.exists():
        await message.answer("❌ Не удалось сделать облегчённую копию БД.")
        return
    size_mb = path.stat().st_size / (1024 * 1024)
    await message.answer_document(
        FSInputFile(path),
        caption=(
            "✅ Облегчённая копия БД\n\n"
            f"Размер ZIP: <b>{size_mb:.2f} MB</b>\n"
            "Внутри файл: <code>bot.db</code>\n\n"
            "Балансы, пользователи, операторы и история сохранены. "
            "Старые тяжёлые QR/blob удалены только из копии, живая БД не заменялась."
        )
    )

async def send_db_backup(bot: Bot, reason: str = "auto"):
    channel_id = backup_channel_id()
    if not channel_id:
        return False
    db_path = Path(DB_PATH)
    if not db_path.exists():
        logging.warning("DB backup skipped: DB file not found")
        return False
    try:
        cleanup_database_size(18)
        compact = make_compact_db_copy(18)
        target_path = Path(compact) if compact and Path(compact).exists() else db_path
        size_mb = target_path.stat().st_size / (1024 * 1024)
        caption = (
            "<b>🗄 Автовыгрузка базы данных</b>\n\n"
            f"🕒 {escape(now_str())}\n"
            f"🔖 Причина: <b>{escape(reason)}</b>\n"
            f"📦 Размер: <b>{size_mb:.2f} MB</b>\n\n"
            "Отправлена компактная копия БД."
        )
        await bot.send_document(channel_id, FSInputFile(str(target_path)), caption=caption)
        logging.info("DB backup sent to %s (%s) file=%s size=%.2fMB", channel_id, reason, target_path, size_mb)
        return True
    except Exception:
        logging.exception("send_db_backup failed")
        return False

async def backup_watcher(bot: Bot):
    while True:
        try:
            if is_backup_enabled() and backup_channel_id():
                await send_db_backup(bot, "auto_15m")
        except Exception:
            logging.exception("backup_watcher failed")
        await asyncio.sleep(900)

async def send_log(bot: Bot, text: str):
    logging.info(re.sub(r"<[^>]+>", "", text))
    channel_id = log_channel_id()
    if channel_id:
        try:
            await bot.send_message(channel_id, text)
        except Exception:
            logging.exception("send_log failed")

def resolve_user_input(raw: str):
    raw = (raw or "").strip()
    if not raw:
        return None

    if raw.lstrip("-").isdigit():
        user = db.get_user(int(raw))
        if user:
            return user

    username = raw.lstrip("@").strip().lower()
    if username:
        user = db.find_user_by_username(username)
        if user:
            return user
        user = db.conn.execute(
            "SELECT * FROM users WHERE lower(username)=? OR lower(username) LIKE ? ORDER BY user_id DESC LIMIT 1",
            (username, f"%{username}%"),
        ).fetchone()
        if user:
            return user

    cleaned = re.sub(r"\D", "", raw)
    if cleaned:
        user = db.find_last_user_by_phone(cleaned)
        if user:
            return user
        variants = []
        if cleaned.startswith("8") and len(cleaned) == 11:
            variants += ["7" + cleaned[1:], "+" + "7" + cleaned[1:]]
        elif cleaned.startswith("7") and len(cleaned) == 11:
            variants += ["8" + cleaned[1:], "+" + cleaned]
        else:
            variants += ["+" + cleaned]
        for v in variants:
            user = db.find_last_user_by_phone(v)
            if user:
                return user
    return None


async def create_crypto_invoice(amount: float, description: str = "Treasury top up") -> tuple[Optional[str], Optional[str], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, None, "CRYPTO_PAY_TOKEN не заполнен."
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    payload = {
        "asset": CRYPTO_PAY_ASSET,
        "amount": f"{amount:.2f}",
        "description": description[:1024],
        "allow_anonymous": True,
        "allow_comments": False,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/createInvoice", json=payload, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        result = data.get("result", {})
        return str(result.get("invoice_id") or ""), result.get("pay_url") or result.get("bot_invoice_url"), "Инвойс создан."
    except Exception as e:
        return None, None, f"Ошибка создания инвойса: {e}"

async def get_crypto_invoice(invoice_id: str) -> tuple[Optional[dict], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, "CRYPTO_PAY_TOKEN не заполнен."
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{CRYPTO_PAY_BASE_URL}/getInvoices", params={"invoice_ids": str(invoice_id)}, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        items = data.get("result", {}).get("items", [])
        return (items[0] if items else None), "ok"
    except Exception as e:
        return None, f"Ошибка проверки инвойса: {e}"

async def create_crypto_check(amount: float, user_id: Optional[int] = None) -> tuple[Optional[int], Optional[str], str]:
    if not CRYPTO_PAY_TOKEN:
        return None, None, "CRYPTO_PAY_TOKEN не заполнен, поэтому выдана ручная заявка вместо чека."
    payload = {"asset": CRYPTO_PAY_ASSET, "amount": f"{amount:.2f}"}
    if CRYPTO_PAY_PIN_CHECK_TO_USER and user_id:
        payload["pin_to_user_id"] = int(user_id)
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/createCheck", json=payload, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get("ok"):
            return None, None, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        result = data.get("result", {})
        return result.get('check_id'), result.get("bot_check_url") or result.get("url"), "Чек создан через Crypto Bot."
    except Exception as e:
        return None, None, f"Ошибка создания чека: {e}"


async def delete_crypto_check(check_id: int) -> tuple[bool, str]:
    if not CRYPTO_PAY_TOKEN:
        return False, "CRYPTO_PAY_TOKEN не заполнен."
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTO_PAY_BASE_URL}/deleteCheck", json={"check_id": int(check_id)}, headers=headers, timeout=20) as resp:
                data = await resp.json(content_type=None)
        if not data.get('ok'):
            return False, f"Crypto Pay API error: {data.get('error', 'unknown_error')}"
        return True, "Чек удалён"
    except Exception as e:
        return False, f"Ошибка удаления чека: {e}"



@router.callback_query(F.data.startswith("admin:operator_delete:"))
async def delete_operator_button(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback)
    key = normalize_operator_key(callback.data.split(":", 2)[2])
    if not key or key not in OPERATORS:
        await callback.message.answer("❌ Оператор не найден.", reply_markup=operator_delete_keyboard())
        return
    title = op_title(key)
    hide_operator_everywhere(key)
    logging.info("operator hidden/deleted by admin button key=%s title=%s admin_id=%s", key, title, callback.from_user.id)
    await callback.message.answer(
        f"✅ Оператор удалён/скрыт: <b>{escape(title)}</b>\\n"
        f"key: <code>{escape(key)}</code>\\n\\n"
        "История заявок и старые номера не удалены.",
        reply_markup=operator_delete_keyboard()
    )

@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    touch_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    try:
        parts = (message.text or '').split(maxsplit=1)
        arg = parts[1].strip() if len(parts) > 1 else ''
        if arg.startswith('ref_'):
            ref_id = int(arg.split('_', 1)[1])
            if set_referrer_if_empty(message.from_user.id, ref_id):
                try:
                    await notify_user(message.bot, ref_id, f"<b>👥 Новый реферал</b>\n\nПользователь <b>{escape(message.from_user.full_name)}</b> зарегистрировался по вашей ссылке.")
                except Exception:
                    pass
    except Exception:
        pass
    await state.clear()
    if not await ensure_required_subscription_entity(message, message.bot, message.from_user.id):
        return
    if is_user_blocked(message.from_user.id):
        await remove_reply_keyboard(message)
        await message.answer(blocked_text())
        return
    await remove_reply_keyboard(message)
    await send_banner_message(message, db.get_setting('start_banner_path', START_BANNER), render_start(message.from_user.id), main_menu())


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    try:
        if callback.message:
            await callback.message.delete()
            logging.info("noop close deleted chat_id=%s message_id=%s", callback.message.chat.id, callback.message.message_id)
            await callback.answer("Закрыто")
            return
    except Exception as e:
        logging.warning("noop close delete failed: %s", e)
    try:
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=None)
            logging.info("noop close markup removed chat_id=%s message_id=%s", callback.message.chat.id, callback.message.message_id)
            await callback.answer("Закрыто")
            return
    except Exception as e:
        logging.warning("noop close edit markup failed: %s", e)
    await safe_callback_answer(callback)

@router.callback_query(F.data == "join:check")
async def join_check(callback: CallbackQuery, state: FSMContext):
    if await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await state.clear()
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
        await callback.answer('Подписка подтверждена')
        return
    await callback.answer('Подписка пока не найдена', show_alert=True)

@router.callback_query(F.data == "menu:home")
async def menu_home(callback: CallbackQuery, state: FSMContext):
    touch_user(callback.from_user.id, callback.from_user.username or "", callback.from_user.full_name)
    await state.clear()
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    if is_user_blocked(callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
    else:
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "menu:mirror")
async def mirror_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        render_mirror_menu(callback.from_user.id),
        mirror_menu_kb(),
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data == "mirror:list")
async def mirror_list(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        render_mirror_menu(callback.from_user.id),
        mirror_menu_kb(),
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data == "mirror:create")
async def mirror_create(callback: CallbackQuery, state: FSMContext):
    await state.set_state(MirrorStates.waiting_token)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:mirror")
    kb.adjust(1)
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>🪞 Создание зеркала</b>\n\n"
        "Отправьте <b>API token</b> нового бота от <b>@BotFather</b>.\n"
        "Этот бот будет сохранён как зеркало сервиса без выдачи дополнительных прав.",
        kb.as_markup(),
    )
    await safe_callback_answer(callback)

@router.message(MirrorStates.waiting_token)
async def mirror_token_received(message: Message, state: FSMContext):
    token = (message.text or "").strip()
    if ":" not in token:
        await message.answer("⚠️ Отправьте корректный токен бота от @BotFather.")
        return
    try:
        test_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        me = await test_bot.get_me()
        await test_bot.session.close()
    except Exception:
        await message.answer("❌ Не удалось проверить токен. Проверьте его и попробуйте ещё раз.")
        return
    db.save_mirror(
        message.from_user.id,
        message.from_user.username or "",
        token,
        int(me.id),
        me.username or "",
        me.full_name or "",
    )
    started, info = await start_live_mirror(token)
    await state.clear()
    extra = "Зеркало сразу запущено и уже должно отвечать." if started else f"Зеркало сохранено, но автозапуск сейчас не удался: {escape(str(info))}"
    await send_banner_message(
        message,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>✅ Зеркало сохранено</b>\n\n"
        f"🤖 Бот: @{escape(me.username or '')}\n"
        f"🆔 ID: <code>{me.id}</code>\n\n"
        f"{extra}",
        mirror_menu_kb(),
    )

@router.callback_query(F.data == "menu:my")
async def menu_my(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    items = user_active_queue_items(callback.from_user.id)
    await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id, 0), my_numbers_kb(items, 0))
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("my:page:"))
async def my_numbers_page(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        page = int(callback.data.split(":")[-1])
    except Exception:
        page = 0
    items = user_active_queue_items(callback.from_user.id)
    await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id, page), my_numbers_kb(items, page))
    await safe_callback_answer(callback)

@router.callback_query(F.data == "menu:profile")
async def menu_profile(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    try:
        text = render_profile(callback.from_user.id)
    except Exception:
        logging.exception("profile render failed user_id=%s", callback.from_user.id)
        text = "<b>👤 Профиль</b>\n\nПрофиль временно восстановлен. Попробуйте открыть ещё раз или напишите /start."
    await replace_banner_message(callback, db.get_setting('profile_banner_path', PROFILE_BANNER), text, profile_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data == "menu:ref")
async def menu_ref(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    await replace_banner_message(callback, db.get_setting('profile_banner_path', PROFILE_BANNER), render_referral(callback.from_user.id), referral_kb(callback.from_user.id))
    await safe_callback_answer(callback)

@router.callback_query(F.data == "menu:withdraw")
async def menu_withdraw(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    payout_link = db.get_payout_link(callback.from_user.id)
    if not payout_link:
        kb = InlineKeyboardBuilder()
        kb.button(text="↩️ Назад", callback_data="menu:profile")
        kb.adjust(1)
        await replace_banner_message(callback, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw_setup(), kb.as_markup())
        await state.set_state(WithdrawStates.waiting_payment_link)
    else:
        await state.set_state(WithdrawStates.waiting_amount)
        await replace_banner_message(callback, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
    await safe_callback_answer(callback)

@router.callback_query(F.data == "menu:payout_link")
async def payout_link_cb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_payment_link)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:profile")
    kb.adjust(1)
    await replace_banner_message(
        callback,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        render_withdraw_setup(),
        kb.as_markup(),
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("submit_more:"))
async def submit_more(callback: CallbackQuery, state: FSMContext):
    if is_user_blocked(callback.from_user.id):
        await callback.answer("Аккаунт заблокирован", show_alert=True)
        return
    if not is_numbers_enabled():
        await callback.answer("Сдача номеров выключена", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return
    _, operator_key, mode = parts
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if mode not in {"hold", "no_hold"}:
        await callback.answer("Неизвестный режим", show_alert=True)
        return
    if not is_operator_mode_enabled(operator_key, mode):
        await callback.answer("Сдача по этому оператору и режиму сейчас выключена.", show_alert=True)
        return

    await state.update_data(operator_key=operator_key, mode=mode)
    await state.set_state(SubmitStates.waiting_qr)
    await callback.message.answer(
        "<b>📨 Загрузите следующий QR-код</b>\n\n"
        f"📱 <b>Оператор:</b> {op_html(operator_key)}\n"
        f"🔄 <b>Режим:</b> {mode_label(mode)}\n"
        f"💰 <b>Цена:</b> <b>{usd(get_mode_price(operator_key, mode, callback.from_user.id))}</b>\n\n"
        "Отправьте <b>ещё одно фото QR</b> с подписью-номером другого номера.\n"
        "Когда закончите, нажмите <b>«Я закончил загрузку»</b>.",
        reply_markup=cancel_inline_kb("menu:home"),
    )
    await callback.answer("Можно загружать следующий QR")

def render_esim_picker() -> str:
    lines = ["<b>📲 Выбор оператора</b>", "", "Нажмите нужного оператора ниже:"]
    return "\n".join(lines)


def esim_kb():
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        kb.add(make_operator_button(operator_key=key, callback_data=f"takeop:{key}"))
    kb.adjust(2)
    return kb.as_markup()


@router.callback_query(F.data.startswith("takeop:"))
async def takeop_callback(callback: CallbackQuery):
    if callback.message.chat.type == ChatType.PRIVATE and not is_operator_or_admin(callback.from_user.id):
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    operator_key = callback.data.split(":", 1)[1]
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if callback.message.chat.type == ChatType.PRIVATE:
        await callback.answer("Команда работает только в рабочей группе или топике.", show_alert=True)
        return
    item = next_waiting_for_operator_mode(operator_key, 'hold') or next_waiting_for_operator_mode(operator_key, 'no_hold') or db.take_next_waiting(operator_key, callback.from_user.id)
    if not item:
        await callback.answer("Очередь пуста", show_alert=True)
        return
    # item may already be taken by mode helper; otherwise take it now
    if item['status'] == 'queued':
        if not db.take_queue_item(item['id'], callback.from_user.id):
            await callback.answer("Заявку уже забрали", show_alert=True)
            return
        item = db.get_queue_item(item['id'])
    caption = queue_caption(item) + "\n\n👇 Выберите действие:"
    thread_id = getattr(callback.message, 'message_thread_id', None)
    await send_queue_item_photo_to_chat(
        callback.bot,
        callback.message.chat.id,
        item,
        caption,
        reply_markup=admin_queue_kb(item),
        message_thread_id=thread_id,
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "menu:submit")
async def submit_start_cb(callback: CallbackQuery, state: FSMContext):
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    if is_user_blocked(callback.from_user.id):
        await callback.answer("Аккаунт заблокирован", show_alert=True)
        return
    if not is_numbers_enabled():
        await callback.answer("Сдача номеров выключена", show_alert=True)
        return
    await state.set_state(SubmitStates.waiting_mode)
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b>💫 ESIM Service X 💫</b>\n\n<b>📲 Сдать номер - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "mode:back")
async def mode_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if is_user_blocked(callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
    else:
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("mode:"))
async def choose_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split(":", 1)[1]
    if mode not in {"hold", "no_hold"}:
        await safe_callback_answer(callback)
        return
    await state.update_data(mode=mode)
    await state.set_state(SubmitStates.waiting_operator)
    mode_title = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
    mode_desc = (
        "🔥 <b>Холд</b> — режим работы с временной фиксацией номера.\n"
        "💰 Актуальные ставки смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>."
        if mode == "hold"
        else "🔥 <b>БезХолд</b> — режим работы без времени работы, оплату по режимам смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>."
    )
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        f"<b>Режим выбран: {mode_title}</b>\n\n{mode_desc}\n\n👇 <b>Теперь выберите оператора:</b>",
        operators_kb(mode, "op", "op:back", callback.from_user.id),
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "op:back")
async def op_back(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubmitStates.waiting_mode)
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b>💫 ESIM Service X 💫</b>\n\n<b>📲 Сдать номер - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("op:"))
async def choose_operator(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    operator_key = parts[1]
    mode = parts[2] if len(parts) > 2 else (await state.get_data()).get("mode", "hold")
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if not is_operator_mode_enabled(operator_key, mode):
        await callback.answer("Сдача по этому оператору и режиму сейчас выключена.", show_alert=True)
        return
    await state.update_data(operator_key=operator_key, mode=mode)
    await state.set_state(SubmitStates.waiting_qr)
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>💫 ESIM Service X 💫</b>\n\n<b>📨 Отправьте QR-код - Фото сообщением</b>\n\n👉 <b>Требуется:</b>\n▫️ Фото QR\n▫️ В подписи укажите номер\n\n🔰 <b>Допустимый формат номера:</b>\n<blockquote>+79991234567  «+7»\n79991234567   «7»\n89991234567   «8»</blockquote>\n\nЕсли передумали нажмите ниже - Отмена",
        cancel_inline_kb("op:back"),
    )
    await safe_callback_answer(callback)


@router.message(WithdrawStates.waiting_amount, F.text == "↩️ Назад")
@router.message(WithdrawStates.waiting_payment_link, F.text == "↩️ Назад")
async def global_back(message: Message, state: FSMContext):
    await state.clear()
    await send_banner_message(message, db.get_setting('start_banner_path', START_BANNER), render_start(message.from_user.id), main_menu())


@router.message(SubmitStates.waiting_qr, F.photo)
async def submit_qr(message: Message, state: FSMContext):
    caption = (message.caption or "").strip()
    phone = normalize_phone(caption)
    if not phone:
        await message.answer(
            "⚠️ Номер должен быть только в формате:\n<code>+79991234567</code>\n<code>79991234567</code>\n<code>89991234567</code>",
            reply_markup=cancel_menu(),
        )
        return
    data = await state.get_data()
    operator_key = data.get("operator_key")
    mode = data.get("mode", "hold")
    if operator_key not in OPERATORS:
        await message.answer("⚠️ Оператор не выбран. Начните заново.", reply_markup=main_menu())
        await state.clear()
        return
    touch_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    if phone_already_paid(phone):
        await message.answer("<b>⛔ Этот номер уже был оплачен.</b>\n\nПовторно поставить уже оплаченный номер нельзя.", reply_markup=cancel_inline_kb())
        return
    file_id = message.photo[-1].file_id
    qr_blob, qr_mime, qr_filename = await download_message_photo_bytes(message.bot, file_id)
    item_id = create_queue_item_ext(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.full_name,
        operator_key,
        phone,
        file_id,
        mode,
        getattr(message.bot, "token", BOT_TOKEN),
        qr_blob,
        qr_mime,
        qr_filename,
    )
    save_queue_operator_snapshot(item_id, operator_key)
    await state.update_data(operator_key=operator_key, mode=mode)
    await send_log(
        message.bot,
        f"<b>📥 Новая ESIM заявка</b>\n"
        f"👤 Отправил: <b>{escape(message.from_user.full_name)}</b>\n"
        f"🆔 <code>{message.from_user.id}</code>\n"
        f"🔗 Username: <b>{escape('@' + message.from_user.username) if message.from_user.username else '—'}</b>\n"
        f"🧾 Заявка: <b>#{item_id}</b>\n"
        f"📱 {op_html(operator_key)}\n"
        f"📞 <code>{escape(pretty_phone(phone))}</code>\n"
        f"🔄 {mode_label(mode)}"
    )
    await message.answer(
        "<b>✅ Заявка принята</b>\n\n"
        f"🧾 ID заявки: <b>{item_id}</b>\n"
        f"📱 Оператор: {op_html(operator_key)}\n"
        f"📞 Номер: <code>{pretty_phone(phone)}</code>\n"
        f"💰 Цена: <b>{usd(get_mode_price(operator_key, mode, message.from_user.id))}</b>\n"
        f"🔄 Режим: <b>{'Холд' if mode == 'hold' else 'БезХолд'}</b>",
        reply_markup=submit_result_kb(operator_key, mode),
    )


@router.message(SubmitStates.waiting_qr)
async def submit_not_photo(message: Message):
    await message.answer("<b>⚠️ Отправьте именно фото QR-кода с подписью-номером.</b>", reply_markup=cancel_menu())


@router.message(F.text == "💸 Вывод средств")
async def withdraw_start(message: Message, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_amount)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:home")
    kb.adjust(1)
    await send_banner_message(message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(message.from_user.id), kb.as_markup())


@router.message(WithdrawStates.waiting_payment_link)
async def withdraw_payment_link(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not looks_like_payout_link(raw):
        await message.answer(
            "<b>⚠️ Ссылка не распознана.</b>\n\n"
            "Отправьте именно ссылку на многоразовый счёт CryptoBot.\n"
            "Пример: <code>https://t.me/send?start=IV...</code>",
            reply_markup=cancel_inline_kb("menu:profile"),
        )
        return
    db.set_payout_link(message.from_user.id, raw)
    await state.set_state(WithdrawStates.waiting_amount)
    await send_banner_message(
        message,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        "<b>✅ Счёт для выплат сохранён</b>\n\nТеперь можно оформить вывод.",
        None,
    )
    await send_banner_message(
        message,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        render_withdraw(message.from_user.id),
        cancel_inline_kb("menu:profile"),
    )

@router.message(WithdrawStates.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".")
    try:
        amount = float(raw)
    except Exception:
        user = db.get_user(message.from_user.id)
        balance = float(user["balance"] if user else 0)
        minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
        await message.answer(
            "<b>💸 Вывод средств</b>\n\n"
            f"📉 Минимальный вывод: <b>{usd(minimum)}</b>\n"
            f"💰 Ваш баланс: <b>{usd(balance)}</b>\n\n"
            "⚠️ Введите сумму числом. Например: <code>12.5</code>",
            reply_markup=cancel_inline_kb("menu:profile"),
        )
        return
    minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
    user = db.get_user(message.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount < minimum:
        await message.answer(f"⚠️ <b>Сумма меньше минимальной.</b> Минимум: <b>{usd(minimum)}</b>", reply_markup=cancel_inline_kb("menu:profile"))
        return
    if amount > balance:
        await message.answer("⚠️ <b>Недостаточно средств на балансе.</b>", reply_markup=cancel_inline_kb("menu:profile"))
        return
    await state.clear()
    await message.answer(
        "<b>Подтверждение вывода</b>\n\n"
        f"🗓 Дата: <b>{now_str()}</b>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
        "Подтвердить создание заявки?",
        reply_markup=confirm_withdraw_kb(amount),
    )


@router.callback_query(F.data == "withdraw_cancel")
async def withdraw_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Вывод отменён.")
    await send_banner_message(callback.message, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("withdraw_confirm:"))
async def withdraw_confirm(callback: CallbackQuery):
    amount = float(callback.data.split(":", 1)[1])
    user = db.get_user(callback.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount > balance:
        await callback.answer("Недостаточно средств на балансе", show_alert=True)
        return
    payout_link = (db.get_payout_link(callback.from_user.id) or "").strip()
    if not payout_link:
        await callback.answer("Сначала привяжите счёт для выплат", show_alert=True)
        return
    db.subtract_balance(callback.from_user.id, amount)
    wd_id = db.create_withdrawal(callback.from_user.id, amount)
    username_line = f"\n🔹 Username: @{escape(callback.from_user.username)}" if callback.from_user.username else ""
    text = (
        "<b>📨 Новая заявка на вывод</b>\n\n"
        f"🧾 ID: <b>{wd_id}</b>\n"
        f"👤 Пользователь: <b>{escape(callback.from_user.full_name)}</b>{username_line}\n"
        f"🆔 ID: <code>{callback.from_user.id}</code>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}"
    )
    plain_text = (
        "📨 Новая заявка на вывод\n\n"
        f"ID: {wd_id}\n"
        f"Пользователь: {callback.from_user.full_name}"
        f"{(' @' + callback.from_user.username) if callback.from_user.username else ''}\n"
        f"ID: {callback.from_user.id}\n"
        f"Сумма: {usd(amount)}\n\n"
        f"Счёт для оплаты:\n{payout_link}"
    )
    channel_id = int(db.get_setting("withdraw_channel_id", str(WITHDRAW_CHANNEL_ID)))
    withdraw_thread_id = int(db.get_setting('withdraw_thread_id', '0') or 0)
    sent_ok = False
    try:
        await callback.bot.send_message(
            channel_id,
            text,
            reply_markup=withdraw_admin_kb(wd_id),
            message_thread_id=(withdraw_thread_id or None),
        )
        sent_ok = True
    except Exception:
        logging.exception("send withdraw to channel failed (with topic)")
    if not sent_ok:
        try:
            await callback.bot.send_message(
                channel_id,
                text,
                reply_markup=withdraw_admin_kb(wd_id),
            )
            sent_ok = True
        except Exception:
            logging.exception("send withdraw to channel failed (without topic)")
    if not sent_ok:
        try:
            await callback.bot.send_message(
                channel_id,
                plain_text,
                reply_markup=withdraw_admin_kb(wd_id),
            )
            sent_ok = True
        except Exception:
            logging.exception("send withdraw to channel failed (plain text fallback)")
    await callback.message.edit_text(
        "✅ Заявка на вывод создана. Она отправлена в канал выплат." if sent_ok else "⚠️ Заявка создана, но сообщение в канал выплат не отправилось. Проверь логи и настройки канала."
    )
    await send_banner_message(callback.message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
    await safe_callback_answer(callback)



@router.callback_query(F.data.startswith("wd_ok:"))
async def wd_ok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    payout_link = db.get_payout_link(int(wd["user_id"])) or "—"
    db.set_withdrawal_status(withdraw_id, "approved", callback.from_user.id, payout_link, "approved_waiting_payment")

    await callback.message.edit_text(
        "<b>✅ Заявка на вывод одобрена</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}\n\n"
        "Статус: <b>Ожидает оплаты</b>",
        reply_markup=withdraw_paid_kb(withdraw_id),
    )
    await callback.answer("Одобрено")

@router.callback_query(F.data.startswith("wd_paid:"))
async def wd_paid(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] not in {"pending", "approved"}:
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    payout_link = db.get_payout_link(int(wd["user_id"])) or (wd["payout_check"] if "payout_check" in wd.keys() else "—")
    db.set_withdrawal_status(withdraw_id, "approved", callback.from_user.id, payout_link, "paid")

    try:
        await callback.bot.send_message(
            int(wd["user_id"]),
            "<b>✅ Выплата отправлена</b>\n\n"
            f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n"
            "Статус: <b>Оплачено</b>\n\n"
            "Средства отправлены на ваш привязанный счёт CryptoBot."
        )
    except Exception:
        logging.exception("send withdraw paid notify failed")

    await callback.message.edit_text(
        "<b>✅ Заявка на вывод обработана</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}\n\n"
        "Статус: <b>Оплачено</b>"
    )
    await callback.answer("Оплачено")

@router.callback_query(F.data.startswith("wd_no:"))
async def wd_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    db.add_balance(int(wd["user_id"]), float(wd["amount"]))
    db.set_withdrawal_status(withdraw_id, "rejected", callback.from_user.id, None, "rejected")
    try:
        await callback.bot.send_message(
            int(wd["user_id"]),
            "<b>❌ Заявка на вывод отклонена</b>\n\n"
            f"💸 Сумма возвращена на баланс: <b>{usd(float(wd['amount']))}</b>"
        )
    except Exception:
        logging.exception("send withdraw rejected failed")
    await callback.message.edit_text(
        "<b>❌ Заявка на вывод отклонена</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n"
        "Деньги возвращены на баланс пользователя."
    )
    await callback.answer("Отклонено")

@router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer(render_admin_home(), reply_markup=admin_root_kb())


@router.callback_query(F.data == "admin:home")
async def admin_home(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text(render_admin_home(), reply_markup=admin_root_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:summary")
async def admin_summary(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_summary(), reply_markup=admin_summary_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:summary_by_date")
async def admin_summary_by_date(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_summary_date)
    await callback.message.answer("📅 Введите дату в формате <code>ДД-ММ-ГГГГ</code> или <code>ДД.ММ.ГГГГ</code>.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:treasury")
async def admin_treasury(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_treasury(), reply_markup=treasury_kb())
    await safe_callback_answer(callback)



@router.callback_query(F.data == "admin:treasury_check")
async def admin_treasury_check(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    added = 0.0
    for row in db.list_recent_treasury_invoices(10):
        if row["status"] != "active" or not row["crypto_invoice_id"]:
            continue
        info, _ = await get_crypto_invoice(row["crypto_invoice_id"])
        if info and str(info.get("status", "")).lower() == "paid":
            db.mark_treasury_invoice_paid(int(row["id"]))
            db.add_treasury(float(row["amount"]))
            added += float(row["amount"])
    await callback.message.edit_text(
        render_admin_treasury() + (f"\n\n✅ Подтверждено пополнений: <b>{usd(added)}</b>" if added else "\n\nПлатежей пока не найдено."),
        reply_markup=treasury_kb()
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:withdraws")
async def admin_withdraws(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_withdraws(), reply_markup=admin_back_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:hold")
async def admin_hold(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_hold(), reply_markup=hold_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:prices")
async def admin_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_prices(), reply_markup=prices_kb())
    await safe_callback_answer(callback)



@router.callback_query(F.data == "admin:group_default_prices")
async def admin_group_default_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_group_default_prices(), reply_markup=group_default_prices_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:set_group_default_price:"))
async def admin_set_group_default_price(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, mode, operator_key = callback.data.split(":")
    await state.set_state(AdminStates.waiting_group_default_price_value)
    await state.update_data(group_default_price_mode=mode, group_default_price_key=operator_key)
    await callback.message.answer(f"Введите общий прайс для групп: {op_text(operator_key)} • <b>{mode_label(mode)}</b>")
    await safe_callback_answer(callback)


@router.message(AdminStates.waiting_group_default_price_value)
async def admin_group_default_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or "").replace(",", ".").replace("$", "").strip())
    except Exception:
        await message.answer("Введите цену числом.")
        return
    if value <= 0:
        await message.answer("Цена должна быть больше 0.")
        return
    data = await state.get_data()
    mode = data.get("group_default_price_mode")
    operator_key = data.get("group_default_price_key")
    if mode not in {"hold", "no_hold"} or not operator_key:
        await state.clear()
        await message.answer("❌ Не удалось сохранить цену.")
        return
    db.set_setting(f"group_default_price_{mode}_{operator_key}", str(value))
    await state.clear()
    await message.answer(render_group_default_prices(), reply_markup=group_default_prices_kb())


@router.callback_query(F.data == "admin:group_default_prices")
async def admin_group_default_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_group_default_prices(), reply_markup=group_default_prices_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:set_group_default_price:"))
async def admin_set_group_default_price(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, mode, operator_key = callback.data.split(":")
    await state.set_state(AdminStates.waiting_group_default_price_value)
    await state.update_data(group_default_price_mode=mode, group_default_price_key=operator_key)
    await callback.message.answer(f"Введите общий прайс для групп: {op_text(operator_key)} • <b>{mode_label(mode)}</b>")
    await safe_callback_answer(callback)


@router.message(AdminStates.waiting_group_default_price_value)
async def admin_group_default_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or "").replace(",", ".").replace("$", "").strip())
    except Exception:
        await message.answer("Введите цену числом.")
        return
    if value <= 0:
        await message.answer("Цена должна быть больше 0.")
        return
    data = await state.get_data()
    mode = data.get("group_default_price_mode")
    operator_key = data.get("group_default_price_key")
    if mode not in {"hold", "no_hold"} or not operator_key:
        await state.clear()
        await message.answer("❌ Не удалось сохранить цену.")
        return
    db.set_setting(f"group_default_price_{mode}_{operator_key}", str(value))
    await state.clear()
    await message.answer(render_group_default_prices(), reply_markup=group_default_prices_kb())


@router.callback_query(F.data == "admin:group_default_prices")
async def admin_group_default_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_group_default_prices(), reply_markup=group_default_prices_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:set_group_default_price:"))
async def admin_set_group_default_price(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, mode, operator_key = callback.data.split(":")
    await state.set_state(AdminStates.waiting_group_default_price_value)
    await state.update_data(group_default_price_mode=mode, group_default_price_key=operator_key)
    await callback.message.answer(f"Введите общий прайс для групп: {op_text(operator_key)} • <b>{mode_label(mode)}</b>")
    await safe_callback_answer(callback)


@router.message(AdminStates.waiting_group_default_price_value)
async def admin_group_default_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or "").replace(",", ".").replace("$", "").strip())
    except Exception:
        await message.answer("Введите цену числом.")
        return
    if value <= 0:
        await message.answer("Цена должна быть больше 0.")
        return
    data = await state.get_data()
    mode = data.get("group_default_price_mode")
    operator_key = data.get("group_default_price_key")
    if mode not in {"hold", "no_hold"} or not operator_key:
        await state.clear()
        await message.answer("❌ Не удалось сохранить цену.")
        return
    db.set_setting(f"group_default_price_{mode}_{operator_key}", str(value))
    await state.clear()
    await message.answer(render_group_default_prices(), reply_markup=group_default_prices_kb())

@router.callback_query(F.data == "admin:roles")
async def admin_roles(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_roles(), reply_markup=roles_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:workspaces")
async def admin_workspaces(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_workspaces(), reply_markup=workspaces_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:group_stats_panel")
async def admin_group_stats_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, "<b>📈 Выберите группу / топик для статистики:</b>", reply_markup=group_stats_list_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:groupstat:"))
async def admin_groupstat_open(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread = int(thread_id)
    thread = None if thread == 0 else thread
    await safe_edit_or_send(callback, render_single_group_stats(chat_id, thread), reply_markup=single_group_stats_kb(chat_id, thread))
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:group_remove:"))
async def admin_group_remove_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread = None if int(thread_id) == 0 else int(thread_id)
    title = workspace_display_title(chat_id, thread)
    if thread is None:
        db.conn.execute("DELETE FROM workspaces WHERE chat_id=?", (chat_id,))
        db.conn.execute("DELETE FROM group_finance WHERE chat_id=?", (chat_id,))
        db.conn.execute("DELETE FROM group_operator_prices WHERE chat_id=?", (chat_id,))
    else:
        thread_key = db._thread_key(thread)
        db.conn.execute("DELETE FROM workspaces WHERE chat_id=? AND thread_id=?", (chat_id, thread_key))
        db.conn.execute("DELETE FROM group_finance WHERE chat_id=? AND thread_id=?", (chat_id, thread_key))
        db.conn.execute("DELETE FROM group_operator_prices WHERE chat_id=? AND thread_id=?", (chat_id, thread_key))
    db.conn.commit()
    left = db.conn.execute("SELECT COUNT(*) AS c FROM workspaces WHERE chat_id=?", (chat_id,)).fetchone()
    logging.info("admin_group_remove chat_id=%s thread_id=%s by user_id=%s title=%s left=%s", chat_id, db._thread_key(thread), callback.from_user.id, title, int((left['c'] if left else 0) or 0))
    await state.clear()
    await safe_edit_or_send(callback, f"<b>✅ Удалено:</b> {escape(title)}\n\nВыберите следующую группу / топик:", reply_markup=group_stats_list_kb())
    await callback.answer("Удалено")

@router.callback_query(F.data == "admin:settings")
async def admin_settings(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
    await safe_callback_answer(callback)




# required_join_hardfix
@router.callback_query(F.data == "admin:required_join_manage")
async def admin_required_join_manage_hardfix(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(
        callback,
        "<b>👥 Обязательная подписка</b>\n\n"
        "Добавьте канал для обязательной подписки.",
        reply_markup=required_join_manage_kb()
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:required_join_add")
async def admin_required_join_add_hardfix(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_required_join_item)
    await callback.message.answer(
        "Отправьте ID канала и ссылку через пробел.\n\n"
        "Пример:\n"
        "<code>-1001234567890 https://t.me/example</code>"
    )
    await safe_callback_answer(callback)


@router.message(AdminStates.waiting_required_join_item)
async def admin_required_join_item_hardfix(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)

    if not parts:
        await message.answer("❌ Неверный формат.")
        return

    try:
        chat_id = int(parts[0])
    except Exception:
        await message.answer("❌ ID канала должен быть числом.")
        return

    link = parts[1].strip() if len(parts) > 1 else ""

    items = required_join_entries()
    exists = False

    for item in items:
        if int(item.get("chat_id", 0)) == chat_id:
            item["link"] = link
            exists = True
            break

    if not exists:
        items.append({
            "chat_id": chat_id,
            "link": link,
            "title": ""
        })

    save_required_join_entries(items)

    await state.clear()

    await message.answer(
        "✅ Канал обязательной подписки добавлен.\n\n"
        f"ID: <code>{chat_id}</code>\n"
        f"Ссылка: {escape(link) if link else '—'}"
    )

@router.callback_query(F.data == "admin:set_withdraw_channel")
async def admin_set_withdraw_channel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await ask_admin_channel_setting(callback, state, "withdraw_channel_id", "💳 <b>Канал выплат</b>")


@router.callback_query(F.data == "admin:set_withdraw_topic")
async def admin_set_withdraw_topic(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await ask_admin_channel_setting(callback, state, "withdraw_thread_id", "🧵 <b>Топик выплат</b>")


@router.callback_query(F.data == "admin:set_log_channel")
async def admin_set_log_channel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await ask_admin_channel_setting(callback, state, "log_channel_id", "🧾 <b>Канал логов</b>")


@router.callback_query(F.data == "admin:set_backup_channel")
async def admin_set_backup_channel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await ask_admin_channel_setting(callback, state, "backup_channel_id", "🗄 <b>Канал автобэкапа</b>")


@router.callback_query(F.data == "admin:toggle_backup")
async def admin_toggle_backup(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    enabled = not is_backup_enabled()
    set_backup_enabled(enabled)
    await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
    await callback.answer("Автовыгрузка включена" if enabled else "Автовыгрузка выключена")


@router.callback_query(F.data == "admin:operator_modes")
async def admin_operator_modes(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:toggle_avail:"))
async def admin_toggle_avail(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, mode, operator_key = callback.data.split(":")
    set_operator_mode_enabled(operator_key, mode, not is_operator_mode_enabled(operator_key, mode))
    await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
    await callback.answer("Статус обновлён")


@router.callback_query(F.data == "admin:design")
async def admin_design(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_design(), reply_markup=design_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:templates")
async def admin_templates(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_templates(), reply_markup=design_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_broadcast(), reply_markup=broadcast_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast_write")
async def admin_broadcast_write(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_broadcast_text)
    await callback.message.answer(
        "Отправьте текст рассылки одним сообщением.\n\nМожно использовать HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast_preview")
async def admin_broadcast_preview(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("broadcast_text", "").strip()
    await callback.message.answer(ad or "Рассылка пока пустая.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast_send_ad")
async def admin_broadcast_send_ad(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("broadcast_text", "").strip()
    if not ad:
        await callback.answer("Сначала сохрани рассылку", show_alert=True)
        return
    sent = 0
    for uid in db.all_user_ids():
        try:
            await callback.bot.send_message(uid, ad)
            sent += 1
        except Exception:
            pass
    await callback.message.answer(f"✅ Рассылка завершена. Доставлено: <b>{sent}</b>")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:usernames")
async def admin_usernames(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    content = db.export_usernames().encode("utf-8")
    file = BufferedInputFile(content, filename="usernames.txt")
    await callback.message.answer_document(file, caption="📥 Собранные username и user_id")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:download_db")
async def admin_download_db(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    path = Path(DB_PATH)
    if not path.exists():
        await callback.answer("База не найдена", show_alert=True)
        return
    await callback.message.answer_document(FSInputFile(path), caption="<b>📦 SQLite база</b>")
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:upload_db")
async def admin_upload_db(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_db_upload)
    await callback.message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_start_text")
async def admin_set_start_text(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_start_text)
    await callback.message.answer(
        "Отправьте новый стартовый текст в формате:\n\n<code>Заголовок\nПодзаголовок\nОписание</code>\n\nПервые 2 строки пойдут в шапку, остальное в описание."
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_ad_text")
async def admin_set_ad_text(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_ad_text)
    await callback.message.answer(
        "Отправьте текст рассылки.\n\nМожно писать красивыми шаблонами и использовать HTML Telegram."
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_operator_emoji")
async def admin_set_operator_emoji_panel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await safe_edit_or_send(
        callback,
        "<b>💎 Эмодзи операторов</b>\n\nВыберите оператора. После этого отправьте <b>premium emoji</b>, <b>стикер</b> с ним, <b>ID</b> или <code>skip</code>, чтобы убрать premium emoji.",
        reply_markup=operator_emoji_pick_kb(),
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:pick_operator_emoji:"))
async def admin_pick_operator_emoji(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    key = callback.data.split(":", 2)[-1].strip().lower()
    if not key or key not in OPERATORS:
        await callback.answer("Оператор не найден", show_alert=True)
        return
    current_emoji_id, current_fallback = CUSTOM_OPERATOR_EMOJI.get(key, ("", "📱"))
    await state.update_data(new_operator_payload={
        'key': key,
        'title': OPERATORS[key].get('title', key),
        'price': float(OPERATORS[key].get('price', 0) or 0),
        'command': OPERATORS[key].get('command', f'/{key}'),
        'emoji': current_fallback or '📱',
        'emoji_id': current_emoji_id or '',
        'edit_existing_operator_emoji': True,
    })
    await state.set_state(AdminStates.waiting_new_operator_emoji)
    await safe_edit_or_send(
        callback,
        f"<b>💎 Эмодзи для оператора</b>\n\nОператор: <b>{escape(OPERATORS[key].get('title', key))}</b>\nТекущий emoji_id: <code>{escape(current_emoji_id or 'нет')}</code>\n\nОтправьте <b>premium emoji</b>, <b>стикер</b> с ним или просто <b>ID</b>.\nОтправьте <code>skip</code>, чтобы убрать premium emoji и оставить обычный смайл.",
        reply_markup=admin_back_kb("admin:set_operator_emoji"),
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:add_operator")
async def admin_add_operator(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_new_operator)
    await callback.message.answer(
        "<b>➕ Добавление оператора</b>\n\n"
        "Отправьте данные в формате:\n\n<code>key | Название | цена</code>\n\n"
        "Пример:\n<code>sber | Сбер | 4.5</code>\n\n"
        "После этого бот отдельно попросит <b>premium emoji ID</b>.\n"
        "Команду указывать не нужно — она будет создана автоматически как <code>/key</code>."
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:remove_operator")
async def admin_remove_operator(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_remove_operator)
    removable = []
    base_keys = set(visible_operator_keys())
    for key, data in OPERATORS.items():
        if key not in base_keys:
            removable.append(f"• <code>{key}</code> — {escape(data.get('title', key))}")
    removable_text = "\n".join(removable) if removable else "• Нет добавленных операторов для удаления."
    await callback.message.answer(
        "<b>➖ Удаление оператора</b>\n\n"
        "Отправьте <code>key</code> оператора, которого нужно удалить.\n\n"
        f"{removable_text}\n\n"
        "Можно удалить любого оператора, включая базового. История номеров не удаляется."
    , reply_markup=operator_delete_keyboard())
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:set_hold")
async def admin_set_hold(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_hold)
    await callback.message.answer("Введите новый Холд в минутах:")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_min_withdraw")
async def admin_set_min_withdraw(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_min_withdraw)
    await callback.message.answer("Введите новый минимальный вывод в $:")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:treasury_add")
async def admin_treasury_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_invoice)
    await callback.message.answer("Введите сумму пополнения казны в $ для создания <b>Crypto Bot invoice</b>:")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:treasury_sub")
async def admin_treasury_sub(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_sub)
    await callback.message.answer("Введите сумму вывода казны в $ — будет создан <b>реальный чек Crypto Bot</b>:")
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:set_price:"))
async def admin_set_price_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split(":")
    if len(parts) == 4:
        _, _, price_mode, operator_key = parts
    elif len(parts) == 5:
        _, _, _, price_mode, operator_key = parts
    else:
        await callback.answer("Некорректные данные прайса", show_alert=True)
        return
    if operator_key not in OPERATORS or price_mode not in {"hold", "no_hold"}:
        await callback.answer("Некорректные данные прайса", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_operator_price)
    await state.update_data(operator_key=operator_key, price_mode=price_mode)
    await callback.message.answer(f"Введите новую цену для {op_text(operator_key)} • <b>{mode_label(price_mode)}</b> в $:")
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:role:"))
async def admin_role_action(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    role = callback.data.split(":")[-1]
    if role == "chief_admin" and callback.from_user.id != CHIEF_ADMIN_ID:
        await callback.answer("Назначать главного админа может только главный админ.", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_role_user)
    await state.update_data(role_target=role)
    await callback.message.answer("Отправьте ID пользователя, которому нужно назначить роль. Для снятия роли тоже отправьте ID.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:ws_help_group")
async def admin_ws_help_group(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("Чтобы добавить рабочую группу, зайдите в нужную группу и отправьте команду <code>/work</code>.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:ws_help_topic")
async def admin_ws_help_topic(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("Чтобы добавить рабочий топик, зайдите в нужный топик и отправьте команду <code>/topic</code>.")
    await safe_callback_answer(callback)


@router.message(AdminStates.waiting_new_operator)
async def admin_new_operator_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or '').strip()
    parts = [x.strip() for x in raw.split('|')]
    if len(parts) < 3:
        await message.answer("Неверный формат. Пример: <code>sber | Сбер | 4.5</code>")
        return
    key = re.sub(r'[^a-z0-9_]+', '', parts[0].lower())
    title = parts[1].strip()
    if not key or not title:
        await message.answer("Укажите корректный key и название.")
        return
    try:
        price = float(parts[2].replace(',', '.'))
    except Exception:
        await message.answer("Цена должна быть числом.")
        return
    command = f'/{key}'
    await state.update_data(new_operator_payload={'key': key, 'title': title, 'price': price, 'command': command})
    await state.set_state(AdminStates.waiting_new_operator_emoji)
    await message.answer(
        "<b>Шаг 2/2 — premium emoji</b>\n\n"
        f"Для оператора <b>{escape(title)}</b> отправьте <b>premium emoji</b>, <b>стикер</b> с ним или просто <b>ID</b>.\n"
        "Можно отправить <code>skip</code>, если ставить premium emoji не нужно."
    )


@router.message(AdminStates.waiting_new_operator_emoji)
async def admin_new_operator_emoji_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    payload = data.get('new_operator_payload') or {}
    key = str(payload.get('key', '')).strip().lower()
    title = str(payload.get('title', '')).strip()
    command = str(payload.get('command', '')).strip() or f'/{key}'
    price = payload.get('price', 0)
    if not key or not title:
        await state.clear()
        await message.answer("Не удалось сохранить оператора: потерялись данные формы. Попробуйте добавить заново.")
        return

    sticker = message.sticker if getattr(message, 'sticker', None) else None
    custom_ids = extract_custom_emoji_ids(message)
    raw_text = (message.text or message.caption or '').strip()
    emoji_id = ''
    fallback_emoji = extract_custom_emoji_fallback(message)

    if raw_text.lower() not in {'skip', '/skip', 'пропуск', 'нет'}:
        if sticker and getattr(sticker, 'custom_emoji_id', None):
            emoji_id = str(sticker.custom_emoji_id)
            if getattr(sticker, 'emoji', None):
                fallback_emoji = str(sticker.emoji)[:2] or '📱'
        elif custom_ids:
            emoji_id = str(custom_ids[0])
            fallback_emoji = extract_custom_emoji_fallback(message)
        elif raw_text:
            digits = re.sub(r'\D+', '', raw_text)
            if digits:
                emoji_id = digits
            else:
                await message.answer("Пришлите premium emoji, стикер с ним, ID или <code>skip</code>.")
                return

    extra_items = load_extra_operator_items()
    base_keys = set(visible_operator_keys())
    item_payload = {'key': key, 'title': title, 'price': price, 'command': command, 'emoji_id': emoji_id, 'emoji': fallback_emoji}
    updated = False
    for item in extra_items:
        if isinstance(item, dict) and str(item.get('key', '')).strip().lower() == key:
            item.update(item_payload)
            updated = True
            break

    is_base = key in base_keys
    if not is_base and not updated:
        extra_items.append(item_payload)

    if key in OPERATORS:
        OPERATORS[key]['title'] = title
        OPERATORS[key]['price'] = price
        OPERATORS[key]['command'] = command
    else:
        OPERATORS[key] = {'title': title, 'price': price, 'command': command}

    upsert_custom_operator_store(key, title, price, command, emoji_id, fallback_emoji)
    await state.clear()
    suffix = f" • emoji_id: <code>{emoji_id}</code>" if emoji_id else " • обычный смайл"
    result_text = "✅ Эмодзи оператора обновлён" if data.get('edit_existing_operator_emoji') else "✅ Оператор сохранён"
    await message.answer(f"{result_text}: <b>{escape(title)}</b> ({key}){suffix}", reply_markup=admin_root_kb())
@router.message(AdminStates.waiting_remove_operator)
async def admin_remove_operator_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    key = re.sub(r'[^a-z0-9_]+', '', (message.text or '').strip().lower())
    if not key:
        await message.answer("Отправьте key оператора.")
        return
    base_keys = set(visible_operator_keys())
    if key in base_keys:
        await message.answer("Базового оператора удалить нельзя.")
        return
    if not key or key not in OPERATORS:
        await message.answer("Оператор не найден.")
        return
    extra_items = load_extra_operator_items()
    extra_items = [item for item in extra_items if not (isinstance(item, dict) and str(item.get('key', '')).strip().lower() == key)]
    save_extra_operator_items(extra_items)
    title = OPERATORS.get(key, {}).get('title', key)
    try:
        del OPERATORS[key]
    except Exception:
        pass
    try:
        CUSTOM_OPERATOR_EMOJI.pop(key, None)
    except Exception:
        pass
    db.conn.execute("UPDATE custom_operators SET is_deleted=1, updated_at=? WHERE key=?", (now_str(), key))
    # цены/названия оставляем в settings, чтобы старые заявки по этому оператору не пропадали из истории
    db.conn.commit()
    await state.clear()
    await message.answer(f"✅ Оператор удалён: <b>{escape(title)}</b> ({escape(key)})", reply_markup=admin_root_kb())

@router.message(AdminStates.waiting_summary_date)
async def admin_summary_date_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or '').strip()
    m = re.fullmatch(r"(\d{2})[-.](\d{2})[-.](\d{4})", raw)
    if not m:
        await message.answer("⚠️ Формат даты: <code>01-04-2026</code>")
        return
    dd, mm, yyyy = map(int, m.groups())
    try:
        dt = datetime(yyyy, mm, dd)
    except Exception:
        await message.answer("⚠️ Такой даты не существует.")
        return
    start = dt.strftime("%Y-%m-%d 00:00:00")
    end = (dt + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    label = dt.strftime("%d.%m.%Y")
    await state.clear()
    await message.answer(render_admin_summary_for_date(start, end, label), reply_markup=admin_summary_kb())


@router.message(AdminStates.waiting_hold)
async def admin_hold_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = int(float((message.text or '').replace(',', '.')))
    except Exception:
        await message.answer("Введите число.")
        return
    db.set_setting("hold_minutes", str(value))
    await state.clear()
    await message.answer("✅ Холд обновлён.", reply_markup=admin_root_kb())


@router.message(AdminStates.waiting_min_withdraw)
async def admin_min_withdraw_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    db.set_setting("min_withdraw", str(value))
    await state.clear()
    await message.answer("✅ Минимальный вывод обновлён.")


@router.message(AdminStates.waiting_treasury_invoice)
async def admin_treasury_add_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    invoice_id, pay_url, status_msg = await create_crypto_invoice(value, "Treasury top up")
    if not invoice_id or not pay_url:
        await message.answer(f"❌ {status_msg}")
        return
    local_id = db.create_treasury_invoice(value, invoice_id, pay_url, message.from_user.id)
    await state.clear()
    await message.answer(
        "<b>✅ Инвойс на пополнение казны создан</b>\n\n"
        f"🧾 Локальный ID: <b>#{local_id}</b>\n"
        f"💸 Сумма: <b>{usd(value)}</b>\n"
        f"🔗 Ссылка на оплату:\n{pay_url}\n\n"
        "После оплаты зайдите в казну и нажмите <b>Проверить оплату</b>."
    )


@router.message(AdminStates.waiting_treasury_sub)
async def admin_treasury_sub_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    if value > db.get_treasury():
        await message.answer("⚠️ В казне недостаточно средств.")
        return
    check_id, check_url, status_msg = await create_crypto_check(value)
    if not check_id or not check_url:
        await message.answer(f"❌ {status_msg}")
        return
    db.subtract_treasury(value)
    await state.clear()
    await message.answer(
        "<b>✅ Вывод казны создан</b>\n\n"
        f"💸 Сумма: <b>{usd(value)}</b>\n"
        f"🎟 Чек: {check_url}\n"
        f"💰 Остаток казны: <b>{usd(db.get_treasury())}</b>"
    )


@router.message(AdminStates.waiting_operator_price)
async def admin_operator_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    data = await state.get_data()
    operator_key = data.get("operator_key")
    price_mode = data.get("price_mode", "hold")
    if operator_key not in OPERATORS or price_mode not in {"hold", "no_hold"}:
        await state.clear()
        await message.answer("Ошибка данных прайса. Откройте раздел прайсов заново.")
        return
    db.set_setting(f"price_{price_mode}_{operator_key}", str(value))
    await state.clear()
    await message.answer(
        f"✅ Прайс обновлён: {op_text(operator_key)} • <b>{mode_label(price_mode)}</b> = <b>{usd(value)}</b>",
        reply_markup=admin_root_kb(),
    )


@router.message(AdminStates.waiting_role_user)
async def admin_role_user_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        target_id = int((message.text or '').strip())
    except Exception:
        await message.answer("Нужен числовой ID.")
        return
    data = await state.get_data()
    role_target = data.get("role_target")
    if role_target == "remove":
        if target_id == CHIEF_ADMIN_ID:
            await message.answer("Главного админа снять нельзя.")
            await state.clear()
            return
        db.remove_role(target_id)
        await message.answer("✅ Роль снята.")
    else:
        if role_target == "chief_admin" and message.from_user.id != CHIEF_ADMIN_ID:
            await message.answer("Назначать главного админа может только главный админ.")
            await state.clear()
            return
        db.set_role(target_id, role_target)
        await message.answer(f"✅ Роль назначена: {role_target}")
    await state.clear()


@router.message(AdminStates.waiting_start_text)
async def admin_start_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = [x.strip() for x in (message.text or "").splitlines() if x.strip()]
    if len(parts) < 2:
        await message.answer("Нужно минимум 2 строки: заголовок и подзаголовок.")
        return
    db.set_setting("start_title", parts[0])
    db.set_setting("start_subtitle", parts[1])
    db.set_setting("start_description", "\n".join(parts[2:]) if len(parts) > 2 else "")
    await state.clear()
    await message.answer("✅ Стартовое оформление обновлено.")


@router.message(AdminStates.waiting_ad_text)
async def admin_ad_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("broadcast_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Объявление сохранено.")


@router.message(AdminStates.waiting_broadcast_text)
async def admin_broadcast_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("broadcast_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Текст сохранён как активная рассылка. Теперь его можно превьюнуть и разослать из /admin.")


@router.message(Command("work"))
async def enable_work_group(message: Message):
    logging.info("/work received chat_id=%s message_id=%s user_id=%s thread_id=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), getattr(message, "message_thread_id", None))
    if not consume_event_once("cmd_work", message.chat.id, message.message_id):
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в группе.")
        return
    if not message.from_user:
        logging.warning("/work ignored: no from_user chat_id=%s message_id=%s", message.chat.id, message.message_id)
        return
    allowed = is_admin(message.from_user.id) or user_role(message.from_user.id) == "chief_admin"
    member_status = "unknown"
    if not allowed:
        try:
            member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
            member_status = getattr(member, "status", "unknown")
            allowed = member_status in {"creator", "administrator"}
        except Exception:
            logging.exception("/work get_chat_member failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
    logging.info("/work access chat_id=%s user_id=%s allowed=%s role=%s member_status=%s", message.chat.id, message.from_user.id, allowed, user_role(message.from_user.id), member_status)
    if not allowed:
        await message.answer("Команду /work может использовать только админ.")
        return
    try:
        before_rows = debug_workspace_rows(message.chat.id)
        thread_id = getattr(message, "message_thread_id", None)
        logging.info("/work before toggle chat_id=%s thread_id=%s rows=%s", message.chat.id, thread_id, before_rows)
        if db.is_workspace_enabled(message.chat.id, None, "group"):
            db.disable_workspace(message.chat.id, None, "group")
            after_rows = debug_workspace_rows(message.chat.id)
            logging.info("/work disabled chat_id=%s by user_id=%s after_rows=%s", message.chat.id, message.from_user.id, after_rows)
            await message.answer("🛑 Работа в этой группе выключена.")
        else:
            db.enable_workspace(message.chat.id, None, "group", message.from_user.id)
            if thread_id:
                db.enable_workspace(message.chat.id, thread_id, "topic", message.from_user.id)
                logging.info("/work auto-enabled current topic chat_id=%s thread_id=%s by user_id=%s", message.chat.id, thread_id, message.from_user.id)
            after_rows = debug_workspace_rows(message.chat.id)
            logging.info("/work enabled chat_id=%s by user_id=%s after_rows=%s", message.chat.id, message.from_user.id, after_rows)
            set_workspace_title(message.chat.id, None, getattr(message.chat, 'title', None), None)
            if thread_id:
                set_workspace_title(message.chat.id, thread_id, getattr(message.chat, 'title', None), None)
            await message.answer("✅ Эта группа добавлена как рабочая. Операторы и админы теперь могут брать здесь номера.")
    except Exception:
        logging.exception("/work failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
        await message.answer("❌ Ошибка при включении рабочей группы. Лог уже записан в Railway.")


@router.message(Command("topic"))
async def enable_work_topic(message: Message):
    logging.info("/topic received chat_id=%s message_id=%s user_id=%s thread_id=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), getattr(message, "message_thread_id", None))
    if not consume_event_once("cmd_topic", message.chat.id, message.message_id):
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в топике группы.")
        return
    if not message.from_user:
        logging.warning("/topic ignored: no from_user chat_id=%s message_id=%s", message.chat.id, message.message_id)
        return
    allowed = is_admin(message.from_user.id) or user_role(message.from_user.id) == "chief_admin"
    member_status = "unknown"
    if not allowed:
        try:
            member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
            member_status = getattr(member, "status", "unknown")
            allowed = member_status in {"creator", "administrator"}
        except Exception:
            logging.exception("/topic get_chat_member failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
    logging.info("/topic access chat_id=%s user_id=%s allowed=%s role=%s member_status=%s", message.chat.id, message.from_user.id, allowed, user_role(message.from_user.id), member_status)
    if not allowed:
        await message.answer("Команду /topic может использовать только админ.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.answer("Открой нужный топик и выполни /topic внутри него.")
        return
    try:
        if db.is_workspace_enabled(message.chat.id, thread_id, "topic"):
            db.disable_workspace(message.chat.id, thread_id, "topic")
            logging.info("/topic disabled chat_id=%s thread_id=%s by user_id=%s", message.chat.id, thread_id, message.from_user.id)
            await message.answer("🛑 Работа в этом топике выключена.")
        else:
            db.enable_workspace(message.chat.id, thread_id, "topic", message.from_user.id)
            set_workspace_title(message.chat.id, thread_id, getattr(message.chat, 'title', None), None)
            logging.info("/topic enabled chat_id=%s thread_id=%s by user_id=%s", message.chat.id, thread_id, message.from_user.id)
            await message.answer("✅ Этот топик добавлен как рабочий.")
    except Exception:
        logging.exception("/topic failed chat_id=%s thread_id=%s user_id=%s", message.chat.id, thread_id, message.from_user.id)
        await message.answer("❌ Ошибка при включении рабочего топика. Лог уже записан в Railway.")


async def send_next_item_for_operator(message: Message, operator_key: str):
    allowed_actor, actor_reason = await message_actor_can_take_esim(message)
    logging.info("send_next_item actor check chat_id=%s user_id=%s allowed=%s reason=%s", message.chat.id, getattr(message.from_user, "id", None), allowed_actor, actor_reason)
    if not allowed_actor:
        await message.answer("Брать номера могут только операторы, админы бота или админы этой группы.")
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    topic_allowed = db.is_workspace_enabled(message.chat.id, thread_id, "topic") if thread_id else False
    group_allowed = db.is_workspace_enabled(message.chat.id, None, "group")
    allowed = topic_allowed or group_allowed
    logging.info("send_next_item workspace check chat_id=%s thread_id=%s topic_allowed=%s group_allowed=%s allowed=%s rows=%s", message.chat.id, thread_id, topic_allowed, group_allowed, allowed, debug_workspace_rows(message.chat.id))
    if not allowed:
        await message.answer("Эта группа/топик не включены как рабочая зона. Используй /work или /topic от админа.")
        return
    item = db.get_next_queue_item(operator_key)
    if not item:
        await message.answer(f"📭 Для оператора {op_text(operator_key)} очередь пуста.")
        return
    group_price = group_price_for_take(message.chat.id, thread_id, item.operator_key, item.mode)
    if db.get_group_balance(message.chat.id, thread_id) + 1e-9 < group_price:
        await message.answer(f"Недостаточно средств в казне группы. Нужно {usd(group_price)}")
        return
    if not db.reserve_queue_item_for_group(item.id, message.from_user.id, message.chat.id, thread_id, group_price):
        await message.answer("Заявку уже забрали.")
        return
    item = db.get_queue_item(item.id)
    try:
        await send_queue_item_photo_to_chat(message.bot, message.chat.id, item, queue_caption(item), reply_markup=admin_queue_kb(item), message_thread_id=thread_id)
    except Exception:
        db.release_item_reservation(item.id)
        db.conn.execute("UPDATE queue_items SET status='queued', taken_by_admin=NULL, taken_at=NULL WHERE id=?", (item.id,))
        db.conn.commit()
        raise


@router.message(Command("mts", "mtc", "mtspremium", "mtssalon", "bil", "bilsalon", "mega", "megafon", "t2", "tele2", "tele2salon", "sber", "vtb", "gaz", "gazprom", "miranda", "dobro", "dobrosvyz"))
async def legacy_take_commands(message: Message):
    if not is_operator_or_admin(message.from_user.id):
        return
    await message.answer("Команды операторов отключены. Используй <b>/esim</b>.")


@router.message(Command("stickerid", "emojiid", "premiumemojiid"))
async def stickerid_command(message: Message, state: FSMContext):
    raw_cmd = ((message.text or "").split()[0]).split("@")[0].lower()
    if raw_cmd not in {"/stickerid", "/emojiid", "/premiumemojiid"}:
        return
    if not is_admin(message.from_user.id):
        return
    sticker = None
    custom_ids = []
    target = message.reply_to_message or message
    if getattr(target, 'sticker', None):
        sticker = target.sticker
    custom_ids.extend(extract_custom_emoji_ids(target))
    if sticker or custom_ids:
        lines = build_sticker_info_lines(sticker, custom_ids)
        await message.answer("<b>🎟 Данные стикера / emoji</b>\n\n" + "\n".join(lines))
        return
    await state.set_state(EmojiLookupStates.waiting_target)
    await message.answer("<b>🎟 Emoji ID режим</b>\n\nОтправь <b>премиум-стикер</b> или сообщение с <b>premium emoji</b>, и я покажу ID.")

@router.message(EmojiLookupStates.waiting_target)
async def emoji_lookup_waiting(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw_cmd = ((message.text or "").split()[0]).split("@")[0].lower() if (message.text or "").startswith("/") else ""
    if raw_cmd:
        await state.clear()
        # Do not swallow bot commands while waiting for emoji.
        if raw_cmd == "/esim":
            await esim_command(message)
        elif raw_cmd == "/closeholds":
            await closeholds_cmd(message)
        elif raw_cmd == "/clearqueue":
            await clearqueue_cmd(message)
        elif raw_cmd == "/adminclear":
            await adminclear_cmd(message)
        return
    sticker = message.sticker if getattr(message, 'sticker', None) else None
    custom_ids = extract_custom_emoji_ids(message)
    if not sticker and not custom_ids:
        await message.answer("Пришли <b>стикер</b> или сообщение с <b>premium emoji</b>.")
        return
    lines = build_sticker_info_lines(sticker, custom_ids)
    await state.clear()
    await message.answer("<b>🎟 Данные стикера / emoji</b>\n\n" + "\n".join(lines))
@router.message(Command("esim"))
async def esim_command(message: Message):
    logging.info("/esim received chat_id=%s message_id=%s user_id=%s thread_id=%s text=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), getattr(message, "message_thread_id", None), message.text)
    if not consume_event_once("cmd_esim", message.chat.id, message.message_id):
        return
    allowed_actor, actor_reason = await message_actor_can_take_esim(message)
    logging.info("/esim actor check chat_id=%s user_id=%s allowed=%s reason=%s", message.chat.id, getattr(message.from_user, "id", None), allowed_actor, actor_reason)
    if not allowed_actor:
        logging.warning("/esim denied chat_id=%s message_id=%s user_id=%s reason=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), actor_reason)
        await message.answer("⛔ Нет доступа.")
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    topic_allowed = db.is_workspace_enabled(message.chat.id, thread_id, "topic") if thread_id else False
    group_allowed = db.is_workspace_enabled(message.chat.id, None, "group")
    allowed = topic_allowed or group_allowed
    logging.info("/esim workspace check chat_id=%s thread_id=%s topic_allowed=%s group_allowed=%s allowed=%s rows=%s", message.chat.id, thread_id, topic_allowed, group_allowed, allowed, debug_workspace_rows(message.chat.id))
    if not allowed:
        await message.answer("Эта группа или топик не включены как рабочая зона. Используй /work или /topic.")
        return
    await message.answer("<b>📥 Выбор номера ESIM</b>\n\nСначала выберите режим, который нужен:", reply_markup=esim_mode_kb(message.from_user.id))


@router.callback_query(F.data == "menu:payout_link")
async def payout_link_cb(callback: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_payment_link)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:profile")
    kb.adjust(1)
    await replace_banner_message(
        callback,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        render_withdraw_setup(),
        kb.as_markup(),
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("submit_more:"))
async def submit_more(callback: CallbackQuery, state: FSMContext):
    if is_user_blocked(callback.from_user.id):
        await callback.answer("Аккаунт заблокирован", show_alert=True)
        return
    if not is_numbers_enabled():
        await callback.answer("Сдача номеров выключена", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректная кнопка", show_alert=True)
        return
    _, operator_key, mode = parts
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if mode not in {"hold", "no_hold"}:
        await callback.answer("Неизвестный режим", show_alert=True)
        return
    if not is_operator_mode_enabled(operator_key, mode):
        await callback.answer("Сдача по этому оператору и режиму сейчас выключена.", show_alert=True)
        return

    await state.update_data(operator_key=operator_key, mode=mode)
    await state.set_state(SubmitStates.waiting_qr)
    await callback.message.answer(
        "<b>📨 Загрузите следующий QR-код</b>\n\n"
        f"📱 <b>Оператор:</b> {op_html(operator_key)}\n"
        f"🔄 <b>Режим:</b> {mode_label(mode)}\n"
        f"💰 <b>Цена:</b> <b>{usd(get_mode_price(operator_key, mode, callback.from_user.id))}</b>\n\n"
        "Отправьте <b>ещё одно фото QR</b> с подписью-номером другого номера.\n"
        "Когда закончите, нажмите <b>«Я закончил загрузку»</b>.",
        reply_markup=cancel_inline_kb("menu:home"),
    )
    await callback.answer("Можно загружать следующий QR")

def render_esim_picker() -> str:
    lines = ["<b>📲 Выбор оператора</b>", "", "Нажмите нужного оператора ниже:"]
    return "\n".join(lines)


def esim_kb():
    kb = InlineKeyboardBuilder()
    for key in OPERATORS:
        kb.add(make_operator_button(operator_key=key, callback_data=f"takeop:{key}"))
    kb.adjust(2)
    return kb.as_markup()


@router.callback_query(F.data.startswith("takeop:"))
async def takeop_callback(callback: CallbackQuery):
    if not is_operator_or_admin(callback.from_user.id):
        return
    operator_key = callback.data.split(":", 1)[1]
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if callback.message.chat.type == ChatType.PRIVATE:
        await callback.answer("Команда работает только в рабочей группе или топике.", show_alert=True)
        return
    item = next_waiting_for_operator_mode(operator_key, 'hold') or next_waiting_for_operator_mode(operator_key, 'no_hold') or db.take_next_waiting(operator_key, callback.from_user.id)
    if not item:
        await callback.answer("Очередь пуста", show_alert=True)
        return
    # item may already be taken by mode helper; otherwise take it now
    if item['status'] == 'queued':
        if not db.take_queue_item(item['id'], callback.from_user.id):
            await callback.answer("Заявку уже забрали", show_alert=True)
            return
        item = db.get_queue_item(item['id'])
    caption = queue_caption(item) + "\n\n👇 Выберите действие:"
    thread_id = getattr(callback.message, 'message_thread_id', None)
    await send_queue_item_photo_to_chat(
        callback.bot,
        callback.message.chat.id,
        item,
        caption,
        reply_markup=admin_queue_kb(item),
        message_thread_id=thread_id,
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "menu:submit")
async def submit_start_cb(callback: CallbackQuery, state: FSMContext):
    if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля использования бота нужна обязательная подписка на группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>.', required_join_kb().as_markup())
        await safe_callback_answer(callback)
        return
    if is_user_blocked(callback.from_user.id):
        await callback.answer("Аккаунт заблокирован", show_alert=True)
        return
    if not is_numbers_enabled():
        await callback.answer("Сдача номеров выключена", show_alert=True)
        return
    await state.set_state(SubmitStates.waiting_mode)
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b>💫 ESIM Service X 💫</b>\n\n<b>📲 Сдать номер - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "mode:back")
async def mode_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if is_user_blocked(callback.from_user.id):
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
    else:
        await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("mode:"))
async def choose_mode(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split(":", 1)[1]
    if mode not in {"hold", "no_hold"}:
        await safe_callback_answer(callback)
        return
    await state.update_data(mode=mode)
    await state.set_state(SubmitStates.waiting_operator)
    mode_title = "⏳ Холд" if mode == "hold" else "⚡ БезХолд"
    mode_desc = (
        "🔥 <b>Холд</b> — режим работы с временной фиксацией номера.\n"
        "💰 Актуальные ставки смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>."
        if mode == "hold"
        else "🔥 <b>БезХолд</b> — режим работы без времени работы, оплату по режимам смотрите в разделе <b>/start</b> — <b>«Прайсы»</b>."
    )
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        f"<b>Режим выбран: {mode_title}</b>\n\n{mode_desc}\n\n👇 <b>Теперь выберите оператора:</b>",
        operators_kb(mode, "op", "op:back", callback.from_user.id),
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "op:back")
async def op_back(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SubmitStates.waiting_mode)
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b>💫 ESIM Service X 💫</b>\n\n<b>📲 Сдать номер - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("op:"))
async def choose_operator(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    operator_key = parts[1]
    mode = parts[2] if len(parts) > 2 else (await state.get_data()).get("mode", "hold")
    if operator_key not in OPERATORS:
        await callback.answer("Неизвестный оператор", show_alert=True)
        return
    if not is_operator_mode_enabled(operator_key, mode):
        await callback.answer("Сдача по этому оператору и режиму сейчас выключена.", show_alert=True)
        return
    await state.update_data(operator_key=operator_key, mode=mode)
    await state.set_state(SubmitStates.waiting_qr)
    await replace_banner_message(
        callback,
        db.get_setting('start_banner_path', START_BANNER),
        "<b>💫 ESIM Service X 💫</b>\n\n<b>📨 Отправьте QR-код - Фото сообщением</b>\n\n👉 <b>Требуется:</b>\n▫️ Фото QR\n▫️ В подписи укажите номер\n\n🔰 <b>Допустимый формат номера:</b>\n<blockquote>+79991234567  «+7»\n79991234567   «7»\n89991234567   «8»</blockquote>\n\nЕсли передумали нажмите ниже - Отмена",
        cancel_inline_kb("op:back"),
    )
    await safe_callback_answer(callback)


@router.message(WithdrawStates.waiting_amount, F.text == "↩️ Назад")
@router.message(WithdrawStates.waiting_payment_link, F.text == "↩️ Назад")
async def global_back(message: Message, state: FSMContext):
    await state.clear()
    await send_banner_message(message, db.get_setting('start_banner_path', START_BANNER), render_start(message.from_user.id), main_menu())


@router.message(SubmitStates.waiting_qr, F.photo)
async def submit_qr(message: Message, state: FSMContext):
    caption = (message.caption or "").strip()
    phone = normalize_phone(caption)
    if not phone:
        await message.answer(
            "⚠️ Номер должен быть только в формате:\n<code>+79991234567</code>\n<code>79991234567</code>\n<code>89991234567</code>",
            reply_markup=cancel_menu(),
        )
        return
    data = await state.get_data()
    operator_key = data.get("operator_key")
    mode = data.get("mode", "hold")
    if operator_key not in OPERATORS:
        await message.answer("⚠️ Оператор не выбран. Начните заново.", reply_markup=main_menu())
        await state.clear()
        return
    touch_user(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
    if phone_already_paid(phone):
        await message.answer("<b>⛔ Этот номер уже был оплачен.</b>\n\nПовторно поставить уже оплаченный номер нельзя.", reply_markup=cancel_inline_kb())
        return
    file_id = message.photo[-1].file_id
    qr_blob, qr_mime, qr_filename = await download_message_photo_bytes(message.bot, file_id)
    item_id = create_queue_item_ext(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.full_name,
        operator_key,
        phone,
        file_id,
        mode,
        getattr(message.bot, "token", BOT_TOKEN),
        qr_blob,
        qr_mime,
        qr_filename,
    )
    save_queue_operator_snapshot(item_id, operator_key)
    await state.update_data(operator_key=operator_key, mode=mode)
    await send_log(
        message.bot,
        f"<b>📥 Новая ESIM заявка</b>\n"
        f"👤 Отправил: <b>{escape(message.from_user.full_name)}</b>\n"
        f"🆔 <code>{message.from_user.id}</code>\n"
        f"🔗 Username: <b>{escape('@' + message.from_user.username) if message.from_user.username else '—'}</b>\n"
        f"🧾 Заявка: <b>#{item_id}</b>\n"
        f"📱 {op_html(operator_key)}\n"
        f"📞 <code>{escape(pretty_phone(phone))}</code>\n"
        f"🔄 {mode_label(mode)}"
    )
    await message.answer(
        "<b>✅ Заявка принята</b>\n\n"
        f"🧾 ID заявки: <b>{item_id}</b>\n"
        f"📱 Оператор: {op_html(operator_key)}\n"
        f"📞 Номер: <code>{pretty_phone(phone)}</code>\n"
        f"💰 Цена: <b>{usd(get_mode_price(operator_key, mode, message.from_user.id))}</b>\n"
        f"🔄 Режим: <b>{'Холд' if mode == 'hold' else 'БезХолд'}</b>",
        reply_markup=submit_result_kb(operator_key, mode),
    )


@router.message(SubmitStates.waiting_qr)
async def submit_not_photo(message: Message):
    await message.answer("<b>⚠️ Отправьте именно фото QR-кода с подписью-номером.</b>", reply_markup=cancel_menu())


@router.message(F.text == "💸 Вывод средств")
async def withdraw_start(message: Message, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_amount)
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Назад", callback_data="menu:home")
    kb.adjust(1)
    await send_banner_message(message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(message.from_user.id), kb.as_markup())


@router.message(WithdrawStates.waiting_payment_link)
async def withdraw_payment_link(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    if not looks_like_payout_link(raw):
        await message.answer(
            "<b>⚠️ Ссылка не распознана.</b>\n\n"
            "Отправьте именно ссылку на многоразовый счёт CryptoBot.\n"
            "Пример: <code>https://t.me/send?start=IV...</code>",
            reply_markup=cancel_inline_kb("menu:profile"),
        )
        return
    db.set_payout_link(message.from_user.id, raw)
    await state.set_state(WithdrawStates.waiting_amount)
    await send_banner_message(
        message,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        "<b>✅ Счёт для выплат сохранён</b>\n\nТеперь можно оформить вывод.",
        None,
    )
    await send_banner_message(
        message,
        db.get_setting('withdraw_banner_path', WITHDRAW_BANNER),
        render_withdraw(message.from_user.id),
        cancel_inline_kb("menu:profile"),
    )

@router.message(WithdrawStates.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    raw = (message.text or "").strip().replace(",", ".")
    try:
        amount = float(raw)
    except Exception:
        user = db.get_user(message.from_user.id)
        balance = float(user["balance"] if user else 0)
        minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
        await message.answer(
            "<b>💸 Вывод средств</b>\n\n"
            f"📉 Минимальный вывод: <b>{usd(minimum)}</b>\n"
            f"💰 Ваш баланс: <b>{usd(balance)}</b>\n\n"
            "⚠️ Введите сумму числом. Например: <code>12.5</code>",
            reply_markup=cancel_inline_kb("menu:profile"),
        )
        return
    minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
    user = db.get_user(message.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount < minimum:
        await message.answer(f"⚠️ <b>Сумма меньше минимальной.</b> Минимум: <b>{usd(minimum)}</b>", reply_markup=cancel_inline_kb("menu:profile"))
        return
    if amount > balance:
        await message.answer("⚠️ <b>Недостаточно средств на балансе.</b>", reply_markup=cancel_inline_kb("menu:profile"))
        return
    await state.clear()
    await message.answer(
        "<b>Подтверждение вывода</b>\n\n"
        f"🗓 Дата: <b>{now_str()}</b>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
        "Подтвердить создание заявки?",
        reply_markup=confirm_withdraw_kb(amount),
    )


@router.callback_query(F.data == "withdraw_cancel")
async def withdraw_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Вывод отменён.")
    await send_banner_message(callback.message, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("withdraw_confirm:"))
async def withdraw_confirm(callback: CallbackQuery):
    amount = float(callback.data.split(":", 1)[1])
    user = db.get_user(callback.from_user.id)
    balance = float(user["balance"] if user else 0)
    if amount > balance:
        await callback.answer("Недостаточно средств на балансе", show_alert=True)
        return
    payout_link = (db.get_payout_link(callback.from_user.id) or "").strip()
    if not payout_link:
        await callback.answer("Сначала привяжите счёт для выплат", show_alert=True)
        return
    db.subtract_balance(callback.from_user.id, amount)
    wd_id = db.create_withdrawal(callback.from_user.id, amount)
    username_line = f"\n🔹 Username: @{escape(callback.from_user.username)}" if callback.from_user.username else ""
    text = (
        "<b>📨 Новая заявка на вывод</b>\n\n"
        f"🧾 ID: <b>{wd_id}</b>\n"
        f"👤 Пользователь: <b>{escape(callback.from_user.full_name)}</b>{username_line}\n"
        f"🆔 ID: <code>{callback.from_user.id}</code>\n"
        f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}"
    )
    plain_text = (
        "📨 Новая заявка на вывод\n\n"
        f"ID: {wd_id}\n"
        f"Пользователь: {callback.from_user.full_name}"
        f"{(' @' + callback.from_user.username) if callback.from_user.username else ''}\n"
        f"ID: {callback.from_user.id}\n"
        f"Сумма: {usd(amount)}\n\n"
        f"Счёт для оплаты:\n{payout_link}"
    )
    channel_id = int(db.get_setting("withdraw_channel_id", str(WITHDRAW_CHANNEL_ID)))
    withdraw_thread_id = int(db.get_setting('withdraw_thread_id', '0') or 0)
    sent_ok = False
    try:
        await callback.bot.send_message(
            channel_id,
            text,
            reply_markup=withdraw_admin_kb(wd_id),
            message_thread_id=(withdraw_thread_id or None),
        )
        sent_ok = True
    except Exception:
        logging.exception("send withdraw to channel failed (with topic)")
    if not sent_ok:
        try:
            await callback.bot.send_message(
                channel_id,
                text,
                reply_markup=withdraw_admin_kb(wd_id),
            )
            sent_ok = True
        except Exception:
            logging.exception("send withdraw to channel failed (without topic)")
    if not sent_ok:
        try:
            await callback.bot.send_message(
                channel_id,
                plain_text,
                reply_markup=withdraw_admin_kb(wd_id),
            )
            sent_ok = True
        except Exception:
            logging.exception("send withdraw to channel failed (plain text fallback)")
    await callback.message.edit_text(
        "✅ Заявка на вывод создана. Она отправлена в канал выплат." if sent_ok else "⚠️ Заявка создана, но сообщение в канал выплат не отправилось. Проверь логи и настройки канала."
    )
    await send_banner_message(callback.message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
    await safe_callback_answer(callback)



@router.callback_query(F.data.startswith("wd_ok:"))
async def wd_ok(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    payout_link = db.get_payout_link(int(wd["user_id"])) or "—"
    db.set_withdrawal_status(withdraw_id, "approved", callback.from_user.id, payout_link, "approved_waiting_payment")

    await callback.message.edit_text(
        "<b>✅ Заявка на вывод одобрена</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}\n\n"
        "Статус: <b>Ожидает оплаты</b>",
        reply_markup=withdraw_paid_kb(withdraw_id),
    )
    await callback.answer("Одобрено")

@router.callback_query(F.data.startswith("wd_paid:"))
async def wd_paid(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] not in {"pending", "approved"}:
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    payout_link = db.get_payout_link(int(wd["user_id"])) or (wd["payout_check"] if "payout_check" in wd.keys() else "—")
    db.set_withdrawal_status(withdraw_id, "approved", callback.from_user.id, payout_link, "paid")

    try:
        await callback.bot.send_message(
            int(wd["user_id"]),
            "<b>✅ Выплата отправлена</b>\n\n"
            f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n"
            "Статус: <b>Оплачено</b>\n\n"
            "Средства отправлены на ваш привязанный счёт CryptoBot."
        )
    except Exception:
        logging.exception("send withdraw paid notify failed")

    await callback.message.edit_text(
        "<b>✅ Заявка на вывод обработана</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n\n"
        f"💳 <b>Счёт для оплаты:</b>\n{escape(payout_link)}\n\n"
        "Статус: <b>Оплачено</b>"
    )
    await callback.answer("Оплачено")

@router.callback_query(F.data.startswith("wd_no:"))
async def wd_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    withdraw_id = int(callback.data.split(":")[-1])
    wd = db.get_withdrawal(withdraw_id)
    if not wd or wd["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return
    db.add_balance(int(wd["user_id"]), float(wd["amount"]))
    db.set_withdrawal_status(withdraw_id, "rejected", callback.from_user.id, None, "rejected")
    try:
        await callback.bot.send_message(
            int(wd["user_id"]),
            "<b>❌ Заявка на вывод отклонена</b>\n\n"
            f"💸 Сумма возвращена на баланс: <b>{usd(float(wd['amount']))}</b>"
        )
    except Exception:
        logging.exception("send withdraw rejected failed")
    await callback.message.edit_text(
        "<b>❌ Заявка на вывод отклонена</b>\n\n"
        f"🧾 ID: <b>{withdraw_id}</b>\n"
        f"👤 Пользователь: <code>{wd['user_id']}</code>\n"
        f"💸 Сумма: <b>{usd(float(wd['amount']))}</b>\n"
        "Деньги возвращены на баланс пользователя."
    )
    await callback.answer("Отклонено")

@router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer(render_admin_home(), reply_markup=admin_root_kb())


@router.callback_query(F.data == "admin:home")
async def admin_home(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text(render_admin_home(), reply_markup=admin_root_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:summary")
async def admin_summary(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_summary(), reply_markup=admin_summary_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:summary_by_date")
async def admin_summary_by_date(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_summary_date)
    await callback.message.answer("📅 Введите дату в формате <code>ДД-ММ-ГГГГ</code> или <code>ДД.ММ.ГГГГ</code>.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:treasury")
async def admin_treasury(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_treasury(), reply_markup=treasury_kb())
    await safe_callback_answer(callback)



@router.callback_query(F.data == "admin:treasury_check")
async def admin_treasury_check(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    added = 0.0
    for row in db.list_recent_treasury_invoices(10):
        if row["status"] != "active" or not row["crypto_invoice_id"]:
            continue
        info, _ = await get_crypto_invoice(row["crypto_invoice_id"])
        if info and str(info.get("status", "")).lower() == "paid":
            db.mark_treasury_invoice_paid(int(row["id"]))
            db.add_treasury(float(row["amount"]))
            added += float(row["amount"])
    await callback.message.edit_text(
        render_admin_treasury() + (f"\n\n✅ Подтверждено пополнений: <b>{usd(added)}</b>" if added else "\n\nПлатежей пока не найдено."),
        reply_markup=treasury_kb()
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:withdraws")
async def admin_withdraws(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_admin_withdraws(), reply_markup=admin_back_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:hold")
async def admin_hold(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_hold(), reply_markup=hold_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:prices")
async def admin_prices(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_prices(), reply_markup=prices_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:roles")
async def admin_roles(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_roles(), reply_markup=roles_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:workspaces")
async def admin_workspaces(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_workspaces(), reply_markup=workspaces_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:group_stats_panel")
async def admin_group_stats_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, "<b>📈 Выберите группу / топик для статистики:</b>", reply_markup=group_stats_list_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:groupstat:"))
async def admin_groupstat_open(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread = int(thread_id)
    thread = None if thread == 0 else thread
    await safe_edit_or_send(callback, render_single_group_stats(chat_id, thread), reply_markup=single_group_stats_kb(chat_id, thread))
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:group_remove:"))
async def admin_group_remove_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread = None if int(thread_id) == 0 else int(thread_id)
    title = workspace_display_title(chat_id, thread)
    if thread is None:
        db.conn.execute("DELETE FROM workspaces WHERE chat_id=?", (chat_id,))
        db.conn.execute("DELETE FROM group_finance WHERE chat_id=?", (chat_id,))
        db.conn.execute("DELETE FROM group_operator_prices WHERE chat_id=?", (chat_id,))
    else:
        thread_key = db._thread_key(thread)
        db.conn.execute("DELETE FROM workspaces WHERE chat_id=? AND thread_id=?", (chat_id, thread_key))
        db.conn.execute("DELETE FROM group_finance WHERE chat_id=? AND thread_id=?", (chat_id, thread_key))
        db.conn.execute("DELETE FROM group_operator_prices WHERE chat_id=? AND thread_id=?", (chat_id, thread_key))
    db.conn.commit()
    left = db.conn.execute("SELECT COUNT(*) AS c FROM workspaces WHERE chat_id=?", (chat_id,)).fetchone()
    logging.info("admin_group_remove chat_id=%s thread_id=%s by user_id=%s title=%s left=%s", chat_id, db._thread_key(thread), callback.from_user.id, title, int((left['c'] if left else 0) or 0))
    await state.clear()
    await safe_edit_or_send(callback, f"<b>✅ Удалено:</b> {escape(title)}\n\nВыберите следующую группу / топик:", reply_markup=group_stats_list_kb())
    await callback.answer("Удалено")

@router.callback_query(F.data == "admin:settings")
async def admin_settings(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
    await safe_callback_answer(callback)



@router.callback_query(F.data == "admin:operator_modes")
async def admin_operator_modes(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:toggle_avail:"))
async def admin_toggle_avail(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, mode, operator_key = callback.data.split(":")
    set_operator_mode_enabled(operator_key, mode, not is_operator_mode_enabled(operator_key, mode))
    await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
    await callback.answer("Статус обновлён")


@router.callback_query(F.data == "admin:design")
async def admin_design(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_design(), reply_markup=design_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:templates")
async def admin_templates(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_templates(), reply_markup=design_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(render_broadcast(), reply_markup=broadcast_kb())
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast_write")
async def admin_broadcast_write(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_broadcast_text)
    await callback.message.answer(
        "Отправьте текст рассылки одним сообщением.\n\nМожно использовать HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast_preview")
async def admin_broadcast_preview(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("broadcast_text", "").strip()
    await callback.message.answer(ad or "Рассылка пока пустая.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:broadcast_send_ad")
async def admin_broadcast_send_ad(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ad = db.get_setting("broadcast_text", "").strip()
    if not ad:
        await callback.answer("Сначала сохрани рассылку", show_alert=True)
        return
    sent = 0
    for uid in db.all_user_ids():
        try:
            await callback.bot.send_message(uid, ad)
            sent += 1
        except Exception:
            pass
    await callback.message.answer(f"✅ Рассылка завершена. Доставлено: <b>{sent}</b>")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:usernames")
async def admin_usernames(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    content = db.export_usernames().encode("utf-8")
    file = BufferedInputFile(content, filename="usernames.txt")
    await callback.message.answer_document(file, caption="📥 Собранные username и user_id")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:download_db")
async def admin_download_db(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    path = Path(DB_PATH)
    if not path.exists():
        await callback.answer("База не найдена", show_alert=True)
        return
    await callback.message.answer_document(FSInputFile(path), caption="<b>📦 SQLite база</b>")
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:upload_db")
async def admin_upload_db(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_db_upload)
    await callback.message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_start_text")
async def admin_set_start_text(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_start_text)
    await callback.message.answer(
        "Отправьте новый стартовый текст в формате:\n\n<code>Заголовок\nПодзаголовок\nОписание</code>\n\nПервые 2 строки пойдут в шапку, остальное в описание."
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_ad_text")
async def admin_set_ad_text(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_ad_text)
    await callback.message.answer(
        "Отправьте текст рассылки.\n\nМожно писать красивыми шаблонами и использовать HTML Telegram."
    )
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:add_operator")
async def admin_add_operator(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_new_operator)
    await callback.message.answer(
        "<b>➕ Добавление оператора</b>\n\n"
        "Отправьте данные в формате:\n\n<code>key | Название | цена</code>\n\n"
        "Пример:\n<code>sber | Сбер | 4.5</code>\n\n"
        "После этого бот отдельно попросит <b>premium emoji ID</b>.\n"
        "Команду указывать не нужно — она будет создана автоматически как <code>/key</code>."
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:remove_operator")
async def admin_remove_operator(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_remove_operator)
    removable = []
    base_keys = set(visible_operator_keys())
    for key, data in OPERATORS.items():
        if key not in base_keys:
            removable.append(f"• <code>{key}</code> — {escape(data.get('title', key))}")
    removable_text = "\n".join(removable) if removable else "• Нет добавленных операторов для удаления."
    await callback.message.answer(
        "<b>➖ Удаление оператора</b>\n\n"
        "Отправьте <code>key</code> оператора, которого нужно удалить.\n\n"
        f"{removable_text}\n\n"
        "Можно удалить любого оператора, включая базового. История номеров не удаляется."
    , reply_markup=operator_delete_keyboard())
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:set_hold")
async def admin_set_hold(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_hold)
    await callback.message.answer("Введите новый Холд в минутах:")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:set_min_withdraw")
async def admin_set_min_withdraw(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_min_withdraw)
    await callback.message.answer("Введите новый минимальный вывод в $:")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:treasury_add")
async def admin_treasury_add(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_invoice)
    await callback.message.answer("Введите сумму пополнения казны в $ для создания <b>Crypto Bot invoice</b>:")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:treasury_sub")
async def admin_treasury_sub(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_treasury_sub)
    await callback.message.answer("Введите сумму вывода казны в $ — будет создан <b>реальный чек Crypto Bot</b>:")
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:set_price:"))
async def admin_set_price_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split(":")
    if len(parts) == 4:
        _, _, price_mode, operator_key = parts
    elif len(parts) == 5:
        _, _, _, price_mode, operator_key = parts
    else:
        await callback.answer("Некорректные данные прайса", show_alert=True)
        return
    if operator_key not in OPERATORS or price_mode not in {"hold", "no_hold"}:
        await callback.answer("Некорректные данные прайса", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_operator_price)
    await state.update_data(operator_key=operator_key, price_mode=price_mode)
    await callback.message.answer(f"Введите новую цену для {op_text(operator_key)} • <b>{mode_label(price_mode)}</b> в $:")
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("admin:role:"))
async def admin_role_action(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    role = callback.data.split(":")[-1]
    if role == "chief_admin" and callback.from_user.id != CHIEF_ADMIN_ID:
        await callback.answer("Назначать главного админа может только главный админ.", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_role_user)
    await state.update_data(role_target=role)
    await callback.message.answer("Отправьте ID пользователя, которому нужно назначить роль. Для снятия роли тоже отправьте ID.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:ws_help_group")
async def admin_ws_help_group(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("Чтобы добавить рабочую группу, зайдите в нужную группу и отправьте команду <code>/work</code>.")
    await safe_callback_answer(callback)


@router.callback_query(F.data == "admin:ws_help_topic")
async def admin_ws_help_topic(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.answer("Чтобы добавить рабочий топик, зайдите в нужный топик и отправьте команду <code>/topic</code>.")
    await safe_callback_answer(callback)


@router.message(AdminStates.waiting_new_operator)
async def admin_new_operator_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or '').strip()
    parts = [x.strip() for x in raw.split('|')]
    if len(parts) < 3:
        await message.answer("Неверный формат. Пример: <code>sber | Сбер | 4.5</code>")
        return
    key = re.sub(r'[^a-z0-9_]+', '', parts[0].lower())
    title = parts[1].strip()
    if not key or not title:
        await message.answer("Укажите корректный key и название.")
        return
    try:
        price = float(parts[2].replace(',', '.'))
    except Exception:
        await message.answer("Цена должна быть числом.")
        return
    command = f'/{key}'
    await state.update_data(new_operator_payload={'key': key, 'title': title, 'price': price, 'command': command})
    await state.set_state(AdminStates.waiting_new_operator_emoji)
    await message.answer(
        "<b>Шаг 2/2 — premium emoji</b>\n\n"
        f"Для оператора <b>{escape(title)}</b> отправьте <b>premium emoji</b>, <b>стикер</b> с ним или просто <b>ID</b>.\n"
        "Можно отправить <code>skip</code>, если ставить premium emoji не нужно."
    )


@router.message(AdminStates.waiting_new_operator_emoji)
async def admin_new_operator_emoji_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    payload = data.get('new_operator_payload') or {}
    key = str(payload.get('key', '')).strip().lower()
    title = str(payload.get('title', '')).strip()
    command = str(payload.get('command', '')).strip() or f'/{key}'
    price = payload.get('price', 0)
    if not key or not title:
        await state.clear()
        await message.answer("Не удалось сохранить оператора: потерялись данные формы. Попробуйте добавить заново.")
        return

    sticker = message.sticker if getattr(message, 'sticker', None) else None
    custom_ids = extract_custom_emoji_ids(message)
    raw_text = (message.text or message.caption or '').strip()
    emoji_id = ''
    fallback_emoji = extract_custom_emoji_fallback(message)

    if raw_text.lower() not in {'skip', '/skip', 'пропуск', 'нет'}:
        if sticker and getattr(sticker, 'custom_emoji_id', None):
            emoji_id = str(sticker.custom_emoji_id)
            if getattr(sticker, 'emoji', None):
                fallback_emoji = str(sticker.emoji)[:2] or '📱'
        elif custom_ids:
            emoji_id = str(custom_ids[0])
            fallback_emoji = extract_custom_emoji_fallback(message)
        elif raw_text:
            digits = re.sub(r'\D+', '', raw_text)
            if digits:
                emoji_id = digits
            else:
                await message.answer("Пришлите premium emoji, стикер с ним, ID или <code>skip</code>.")
                return

    extra_items = load_extra_operator_items()
    base_keys = set(visible_operator_keys())
    item_payload = {'key': key, 'title': title, 'price': price, 'command': command, 'emoji_id': emoji_id, 'emoji': fallback_emoji}
    updated = False
    for item in extra_items:
        if isinstance(item, dict) and str(item.get('key', '')).strip().lower() == key:
            item.update(item_payload)
            updated = True
            break

    is_base = key in base_keys
    if not is_base and not updated:
        extra_items.append(item_payload)

    if key in OPERATORS:
        OPERATORS[key]['title'] = title
        OPERATORS[key]['price'] = price
        OPERATORS[key]['command'] = command
    else:
        OPERATORS[key] = {'title': title, 'price': price, 'command': command}

    upsert_custom_operator_store(key, title, price, command, emoji_id, fallback_emoji)
    await state.clear()
    suffix = f" • emoji_id: <code>{emoji_id}</code>" if emoji_id else " • обычный смайл"
    result_text = "✅ Эмодзи оператора обновлён" if data.get('edit_existing_operator_emoji') else "✅ Оператор сохранён"
    await message.answer(f"{result_text}: <b>{escape(title)}</b> ({key}){suffix}", reply_markup=admin_root_kb())


@router.message(AdminStates.waiting_remove_operator)
async def admin_remove_operator_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    key = re.sub(r'[^a-z0-9_]+', '', (message.text or '').strip().lower())
    if not key:
        await message.answer("Отправьте key оператора.")
        return
    base_keys = set(visible_operator_keys())
    if key in base_keys:
        await message.answer("Базового оператора удалить нельзя.")
        return
    if not key or key not in OPERATORS:
        await message.answer("Оператор не найден.")
        return
    extra_items = load_extra_operator_items()
    extra_items = [item for item in extra_items if not (isinstance(item, dict) and str(item.get('key', '')).strip().lower() == key)]
    db.set_setting('extra_operators_json', json.dumps(extra_items, ensure_ascii=False))
    title = OPERATORS.get(key, {}).get('title', key)
    try:
        del OPERATORS[key]
    except Exception:
        pass
    try:
        CUSTOM_OPERATOR_EMOJI.pop(key, None)
    except Exception:
        pass
    db.conn.execute("UPDATE custom_operators SET is_deleted=1, updated_at=? WHERE key=?", (now_str(), key))
    # цены/названия оставляем в settings, чтобы старые заявки по этому оператору не пропадали из истории
    db.conn.commit()
    await state.clear()
    await message.answer(f"✅ Оператор удалён: <b>{escape(title)}</b> ({escape(key)})", reply_markup=admin_root_kb())

@router.message(AdminStates.waiting_summary_date)
async def admin_summary_date_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or '').strip()
    m = re.fullmatch(r"(\d{2})[-.](\d{2})[-.](\d{4})", raw)
    if not m:
        await message.answer("⚠️ Формат даты: <code>01-04-2026</code>")
        return
    dd, mm, yyyy = map(int, m.groups())
    try:
        dt = datetime(yyyy, mm, dd)
    except Exception:
        await message.answer("⚠️ Такой даты не существует.")
        return
    start = dt.strftime("%Y-%m-%d 00:00:00")
    end = (dt + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    label = dt.strftime("%d.%m.%Y")
    await state.clear()
    await message.answer(render_admin_summary_for_date(start, end, label), reply_markup=admin_summary_kb())


@router.message(AdminStates.waiting_hold)
async def admin_hold_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = int(float((message.text or '').replace(',', '.')))
    except Exception:
        await message.answer("Введите число.")
        return
    db.set_setting("hold_minutes", str(value))
    await state.clear()
    await message.answer("✅ Холд обновлён.", reply_markup=admin_root_kb())


@router.message(AdminStates.waiting_min_withdraw)
async def admin_min_withdraw_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    db.set_setting("min_withdraw", str(value))
    await state.clear()
    await message.answer("✅ Минимальный вывод обновлён.")


@router.message(AdminStates.waiting_treasury_invoice)
async def admin_treasury_add_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    invoice_id, pay_url, status_msg = await create_crypto_invoice(value, "Treasury top up")
    if not invoice_id or not pay_url:
        await message.answer(f"❌ {status_msg}")
        return
    local_id = db.create_treasury_invoice(value, invoice_id, pay_url, message.from_user.id)
    await state.clear()
    await message.answer(
        "<b>✅ Инвойс на пополнение казны создан</b>\n\n"
        f"🧾 Локальный ID: <b>#{local_id}</b>\n"
        f"💸 Сумма: <b>{usd(value)}</b>\n"
        f"🔗 Ссылка на оплату:\n{pay_url}\n\n"
        "После оплаты зайдите в казну и нажмите <b>Проверить оплату</b>."
    )


@router.message(AdminStates.waiting_treasury_sub)
async def admin_treasury_sub_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    if value > db.get_treasury():
        await message.answer("⚠️ В казне недостаточно средств.")
        return
    check_id, check_url, status_msg = await create_crypto_check(value)
    if not check_id or not check_url:
        await message.answer(f"❌ {status_msg}")
        return
    db.subtract_treasury(value)
    await state.clear()
    await message.answer(
        "<b>✅ Вывод казны создан</b>\n\n"
        f"💸 Сумма: <b>{usd(value)}</b>\n"
        f"🎟 Чек: {check_url}\n"
        f"💰 Остаток казны: <b>{usd(db.get_treasury())}</b>"
    )


@router.message(AdminStates.waiting_operator_price)
async def admin_operator_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        value = float((message.text or '').replace(',', '.'))
    except Exception:
        await message.answer("Введите число.")
        return
    data = await state.get_data()
    operator_key = data.get("operator_key")
    price_mode = data.get("price_mode", "hold")
    if operator_key not in OPERATORS or price_mode not in {"hold", "no_hold"}:
        await state.clear()
        await message.answer("Ошибка данных прайса. Откройте раздел прайсов заново.")
        return
    db.set_setting(f"price_{price_mode}_{operator_key}", str(value))
    await state.clear()
    await message.answer(
        f"✅ Прайс обновлён: {op_text(operator_key)} • <b>{mode_label(price_mode)}</b> = <b>{usd(value)}</b>",
        reply_markup=admin_root_kb(),
    )


@router.message(AdminStates.waiting_role_user)
async def admin_role_user_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        target_id = int((message.text or '').strip())
    except Exception:
        await message.answer("Нужен числовой ID.")
        return
    data = await state.get_data()
    role_target = data.get("role_target")
    if role_target == "remove":
        if target_id == CHIEF_ADMIN_ID:
            await message.answer("Главного админа снять нельзя.")
            await state.clear()
            return
        db.remove_role(target_id)
        await message.answer("✅ Роль снята.")
    else:
        if role_target == "chief_admin" and message.from_user.id != CHIEF_ADMIN_ID:
            await message.answer("Назначать главного админа может только главный админ.")
            await state.clear()
            return
        db.set_role(target_id, role_target)
        await message.answer(f"✅ Роль назначена: {role_target}")
    await state.clear()


@router.message(AdminStates.waiting_start_text)
async def admin_start_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    parts = [x.strip() for x in (message.text or "").splitlines() if x.strip()]
    if len(parts) < 2:
        await message.answer("Нужно минимум 2 строки: заголовок и подзаголовок.")
        return
    db.set_setting("start_title", parts[0])
    db.set_setting("start_subtitle", parts[1])
    db.set_setting("start_description", "\n".join(parts[2:]) if len(parts) > 2 else "")
    await state.clear()
    await message.answer("✅ Стартовое оформление обновлено.")


@router.message(AdminStates.waiting_ad_text)
async def admin_ad_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("broadcast_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Объявление сохранено.")


@router.message(AdminStates.waiting_broadcast_text)
async def admin_broadcast_text_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    db.set_setting("broadcast_text", message.html_text or (message.text or ""))
    await state.clear()
    await message.answer("✅ Текст сохранён как активная рассылка. Теперь его можно превьюнуть и разослать из /admin.")


@router.message(Command("work"))
async def enable_work_group(message: Message):
    logging.info("/work received chat_id=%s message_id=%s user_id=%s thread_id=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), getattr(message, "message_thread_id", None))
    if not consume_event_once("cmd_work", message.chat.id, message.message_id):
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в группе.")
        return
    if not message.from_user:
        logging.warning("/work ignored: no from_user chat_id=%s message_id=%s", message.chat.id, message.message_id)
        return
    allowed = is_admin(message.from_user.id) or user_role(message.from_user.id) == "chief_admin"
    member_status = "unknown"
    if not allowed:
        try:
            member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
            member_status = getattr(member, "status", "unknown")
            allowed = member_status in {"creator", "administrator"}
        except Exception:
            logging.exception("/work get_chat_member failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
    logging.info("/work access chat_id=%s user_id=%s allowed=%s role=%s member_status=%s", message.chat.id, message.from_user.id, allowed, user_role(message.from_user.id), member_status)
    if not allowed:
        await message.answer("Команду /work может использовать только админ.")
        return
    try:
        before_rows = debug_workspace_rows(message.chat.id)
        thread_id = getattr(message, "message_thread_id", None)
        logging.info("/work before toggle chat_id=%s thread_id=%s rows=%s", message.chat.id, thread_id, before_rows)
        if db.is_workspace_enabled(message.chat.id, None, "group"):
            db.disable_workspace(message.chat.id, None, "group")
            after_rows = debug_workspace_rows(message.chat.id)
            logging.info("/work disabled chat_id=%s by user_id=%s after_rows=%s", message.chat.id, message.from_user.id, after_rows)
            await message.answer("🛑 Работа в этой группе выключена.")
        else:
            db.enable_workspace(message.chat.id, None, "group", message.from_user.id)
            if thread_id:
                db.enable_workspace(message.chat.id, thread_id, "topic", message.from_user.id)
                logging.info("/work auto-enabled current topic chat_id=%s thread_id=%s by user_id=%s", message.chat.id, thread_id, message.from_user.id)
            after_rows = debug_workspace_rows(message.chat.id)
            logging.info("/work enabled chat_id=%s by user_id=%s after_rows=%s", message.chat.id, message.from_user.id, after_rows)
            set_workspace_title(message.chat.id, None, getattr(message.chat, 'title', None), None)
            if thread_id:
                set_workspace_title(message.chat.id, thread_id, getattr(message.chat, 'title', None), None)
            await message.answer("✅ Эта группа добавлена как рабочая. Операторы и админы теперь могут брать здесь номера.")
    except Exception:
        logging.exception("/work failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
        await message.answer("❌ Ошибка при включении рабочей группы. Лог уже записан в Railway.")


@router.message(Command("topic"))
async def enable_work_topic(message: Message):
    logging.info("/topic received chat_id=%s message_id=%s user_id=%s thread_id=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), getattr(message, "message_thread_id", None))
    if not consume_event_once("cmd_topic", message.chat.id, message.message_id):
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Эта команда работает только в топике группы.")
        return
    if not message.from_user:
        logging.warning("/topic ignored: no from_user chat_id=%s message_id=%s", message.chat.id, message.message_id)
        return
    allowed = is_admin(message.from_user.id) or user_role(message.from_user.id) == "chief_admin"
    member_status = "unknown"
    if not allowed:
        try:
            member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
            member_status = getattr(member, "status", "unknown")
            allowed = member_status in {"creator", "administrator"}
        except Exception:
            logging.exception("/topic get_chat_member failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
    logging.info("/topic access chat_id=%s user_id=%s allowed=%s role=%s member_status=%s", message.chat.id, message.from_user.id, allowed, user_role(message.from_user.id), member_status)
    if not allowed:
        await message.answer("Команду /topic может использовать только админ.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    if not thread_id:
        await message.answer("Открой нужный топик и выполни /topic внутри него.")
        return
    try:
        if db.is_workspace_enabled(message.chat.id, thread_id, "topic"):
            db.disable_workspace(message.chat.id, thread_id, "topic")
            logging.info("/topic disabled chat_id=%s thread_id=%s by user_id=%s", message.chat.id, thread_id, message.from_user.id)
            await message.answer("🛑 Работа в этом топике выключена.")
        else:
            db.enable_workspace(message.chat.id, thread_id, "topic", message.from_user.id)
            set_workspace_title(message.chat.id, thread_id, getattr(message.chat, 'title', None), None)
            logging.info("/topic enabled chat_id=%s thread_id=%s by user_id=%s", message.chat.id, thread_id, message.from_user.id)
            await message.answer("✅ Этот топик добавлен как рабочий.")
    except Exception:
        logging.exception("/topic failed chat_id=%s thread_id=%s user_id=%s", message.chat.id, thread_id, message.from_user.id)
        await message.answer("❌ Ошибка при включении рабочего топика. Лог уже записан в Railway.")


async def send_next_item_for_operator(message: Message, operator_key: str):
    allowed_actor, actor_reason = await message_actor_can_take_esim(message)
    logging.info("send_next_item actor check chat_id=%s user_id=%s allowed=%s reason=%s", message.chat.id, getattr(message.from_user, "id", None), allowed_actor, actor_reason)
    if not allowed_actor:
        await message.answer("Брать номера могут только операторы, админы бота или админы этой группы.")
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    topic_allowed = db.is_workspace_enabled(message.chat.id, thread_id, "topic") if thread_id else False
    group_allowed = db.is_workspace_enabled(message.chat.id, None, "group")
    allowed = topic_allowed or group_allowed
    logging.info("send_next_item workspace check chat_id=%s thread_id=%s topic_allowed=%s group_allowed=%s allowed=%s rows=%s", message.chat.id, thread_id, topic_allowed, group_allowed, allowed, debug_workspace_rows(message.chat.id))
    if not allowed:
        await message.answer("Эта группа/топик не включены как рабочая зона. Используй /work или /topic от админа.")
        return
    item = db.get_next_queue_item(operator_key)
    if not item:
        await message.answer(f"📭 Для оператора {op_text(operator_key)} очередь пуста.")
        return
    group_price = group_price_for_take(message.chat.id, thread_id, item.operator_key, item.mode)
    if db.get_group_balance(message.chat.id, thread_id) + 1e-9 < group_price:
        await message.answer(f"Недостаточно средств в казне группы. Нужно {usd(group_price)}")
        return
    if not db.reserve_queue_item_for_group(item.id, message.from_user.id, message.chat.id, thread_id, group_price):
        await message.answer("Заявку уже забрали.")
        return
    item = db.get_queue_item(item.id)
    try:
        await send_queue_item_photo_to_chat(message.bot, message.chat.id, item, queue_caption(item), reply_markup=admin_queue_kb(item), message_thread_id=thread_id)
    except Exception:
        db.release_item_reservation(item.id)
        db.conn.execute("UPDATE queue_items SET status='queued', taken_by_admin=NULL, taken_at=NULL WHERE id=?", (item.id,))
        db.conn.commit()
        raise


@router.message(Command("mts", "mtc", "mtspremium", "mtssalon", "bil", "bilsalon", "mega", "megafon", "t2", "tele2", "tele2salon", "sber", "vtb", "gaz", "gazprom", "miranda", "dobro", "dobrosvyz"))
async def legacy_take_commands(message: Message):
    if not is_operator_or_admin(message.from_user.id):
        return
    await message.answer("Команды операторов отключены. Используй <b>/esim</b>.")



RESERVED_BOT_COMMANDS_FOR_DYNAMIC_STUB = {
    "/start", "/admin", "/esim", "/work", "/topic", "/emojiid",
    "/closeholds", "/clearqueue", "/adminclear", "/stata", "/stats"
}

@router.message(F.text.regexp(r"^/[A-Za-z0-9_]+(?:@\w+)?$"))

@router.message(Command("stata"))
@router.message(Command("stats"))
async def group_stata(message: Message):
    try:
        if message.chat.type == ChatType.PRIVATE:
            return await message.answer("❌ Команда работает только в группах.")

        day_start, day_end, day_label = msk_today_bounds_str()

        active_statuses = "'queued','taken','in_progress','waiting_check','checking','on_hold'"

        total = db.conn.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN taken_at IS NOT NULL THEN 1 ELSE 0 END) AS taken,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS turnover
            FROM queue_items
            WHERE charge_chat_id=?
              AND ((created_at>=? AND created_at<?) OR status IN ({active_statuses}))
            """,
            (message.chat.id, day_start, day_end),
        ).fetchone()

        operators = db.conn.execute(
            f"""
            SELECT
                operator_key,
                COUNT(*) AS total,
                SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
                SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total
            FROM queue_items
            WHERE charge_chat_id=?
              AND ((created_at>=? AND created_at<?) OR status IN ({active_statuses}))
            GROUP BY operator_key
            ORDER BY total DESC
            """,
            (message.chat.id, day_start, day_end),
        ).fetchall()

        lines = [
            "📊 <b>СТАТИСТИКА ГРУППЫ</b>",
            f"🗓 День: <b>{day_label}</b>",
            "",
            f"📦 Всего: <b>{int(total['total'] or 0)}</b>",
            f"🙋 Взято: <b>{int(total['taken'] or 0)}</b>",
            f"✅ Успешно: <b>{int(total['success'] or 0)}</b>",
            f"❌ Слеты: <b>{int(total['slips'] or 0)}</b>",
            f"⚠️ Ошибки: <b>{int(total['errors'] or 0)}</b>",
            f"🏦 Оборот: <b>{usd(total['turnover'] or 0)}</b>",
        ]

        if operators:
            lines.append("")
            lines.append("📱 <b>ПО ОПЕРАТОРАМ</b>")
            for row in operators:
                lines.append(
                    f"• {op_text(row['operator_key'])}: "
                    f"<b>{int(row['total'] or 0)}</b> "
                    f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)})"
                )

        await message.answer("\n".join(lines))

    except Exception as e:
        logging.exception("group_stata hard fail")
        await message.answer(f"❌ Ошибка stata: <code>{escape(str(e))}</code>")


async def dynamic_operator_command_stub(message: Message):
    raw_cmd_for_stub = ((message.text or "").split()[0]).split("@")[0].lower()
    if raw_cmd_for_stub in RESERVED_BOT_COMMANDS_FOR_DYNAMIC_STUB:
        logging.info("dynamic_operator_command_stub reserved skip raw=%s chat_id=%s user_id=%s", raw_cmd_for_stub, message.chat.id, getattr(message.from_user, "id", None))
        return
    raw = (message.text or '').split()[0].split('@')[0].lower()
    if raw in {'/start','/admin','/work','/topic','/esim','/stata'}:
        logging.info("dynamic_operator_command_stub skip raw=%s chat_id=%s user_id=%s", raw, message.chat.id, getattr(message.from_user, 'id', None))
        raise SkipHandler()
    if not message.from_user or not is_operator_or_admin(message.from_user.id):
        raise SkipHandler()
    if raw in operator_command_map():
        logging.info("dynamic_operator_command_stub handled raw=%s chat_id=%s user_id=%s", raw, message.chat.id, message.from_user.id)
        await message.answer("Команды операторов отключены. Используй <b>/esim</b>.")
        return
    raise SkipHandler()



def extract_custom_emoji_ids(message: Message) -> list[str]:
    ids = []
    entities = list(message.entities or []) + list(message.caption_entities or [])
    for ent in entities:
        if getattr(ent, "type", None) == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
            ids.append(ent.custom_emoji_id)
    return ids


def extract_custom_emoji_fallback(message: Message) -> str:
    raw = getattr(message, 'text', None) or getattr(message, 'caption', None) or ''
    entities = list(message.entities or []) + list(message.caption_entities or [])
    for ent in entities:
        if getattr(ent, 'type', None) == 'custom_emoji':
            offset = int(getattr(ent, 'offset', 0) or 0)
            length = int(getattr(ent, 'length', 0) or 0)
            if length > 0 and len(raw) >= offset + length:
                fallback = raw[offset:offset + length].strip()
                if fallback:
                    return fallback[:2]
    sticker = getattr(message, 'sticker', None)
    if sticker and getattr(sticker, 'emoji', None):
        return str(sticker.emoji).strip()[:2] or '📱'
    raw = raw.strip()
    if raw and not raw.isdigit():
        return raw[:2]
    return '📱'

def build_sticker_info_lines(sticker=None, custom_ids=None):
    lines = []
    if sticker:
        lines.append(f"<b>file_id:</b> <code>{sticker.file_id}</code>")
        lines.append(f"<b>file_unique_id:</b> <code>{sticker.file_unique_id}</code>")
        if getattr(sticker, 'set_name', None):
            lines.append(f"<b>set_name:</b> <code>{sticker.set_name}</code>")
        if getattr(sticker, 'emoji', None):
            lines.append(f"<b>emoji:</b> {escape(sticker.emoji)}")
        if getattr(sticker, 'custom_emoji_id', None):
            lines.append(f"<b>custom_emoji_id:</b> <code>{sticker.custom_emoji_id}</code>")
        if getattr(sticker, 'is_animated', None) is not None:
            lines.append(f"<b>animated:</b> <code>{sticker.is_animated}</code>")
        if getattr(sticker, 'is_video', None) is not None:
            lines.append(f"<b>video:</b> <code>{sticker.is_video}</code>")
    for cid in custom_ids or []:
        lines.append(f"<b>custom_emoji_id:</b> <code>{cid}</code>")
    return lines
@router.message(Command("stickerid", "emojiid", "premiumemojiid"))
async def stickerid_command(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    sticker = None
    custom_ids = []
    target = message.reply_to_message or message
    if getattr(target, 'sticker', None):
        sticker = target.sticker
    custom_ids.extend(extract_custom_emoji_ids(target))
    if sticker or custom_ids:
        lines = build_sticker_info_lines(sticker, custom_ids)
        await message.answer("<b>🎟 Данные стикера / emoji</b>\n\n" + "\n".join(lines))
        return
    await state.set_state(EmojiLookupStates.waiting_target)
    await message.answer("<b>🎟 Emoji ID режим</b>\n\nОтправь <b>премиум-стикер</b> или сообщение с <b>premium emoji</b>, и я покажу ID.")

@router.message(EmojiLookupStates.waiting_target)
async def emoji_lookup_waiting(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw_cmd = ((message.text or "").split()[0]).split("@")[0].lower() if (message.text or "").startswith("/") else ""
    if raw_cmd:
        await state.clear()
        # Do not swallow bot commands while waiting for emoji.
        if raw_cmd == "/esim":
            await esim_command(message)
        elif raw_cmd == "/closeholds":
            await closeholds_cmd(message)
        elif raw_cmd == "/clearqueue":
            await clearqueue_cmd(message)
        elif raw_cmd == "/adminclear":
            await adminclear_cmd(message)
        return
    sticker = message.sticker if getattr(message, 'sticker', None) else None
    custom_ids = extract_custom_emoji_ids(message)
    if not sticker and not custom_ids:
        await message.answer("Пришли <b>стикер</b> или сообщение с <b>premium emoji</b>.")
        return
    lines = build_sticker_info_lines(sticker, custom_ids)
    await state.clear()
    await message.answer("<b>🎟 Данные стикера / emoji</b>\n\n" + "\n".join(lines))
@router.message(Command("esim"))
async def esim_command(message: Message):
    logging.info("/esim received chat_id=%s message_id=%s user_id=%s thread_id=%s text=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), getattr(message, "message_thread_id", None), message.text)
    if not consume_event_once("cmd_esim", message.chat.id, message.message_id):
        return
    allowed_actor, actor_reason = await message_actor_can_take_esim(message)
    logging.info("/esim actor check chat_id=%s user_id=%s allowed=%s reason=%s", message.chat.id, getattr(message.from_user, "id", None), allowed_actor, actor_reason)
    if not allowed_actor:
        logging.warning("/esim denied chat_id=%s message_id=%s user_id=%s reason=%s", message.chat.id, message.message_id, getattr(message.from_user, "id", None), actor_reason)
        await message.answer("⛔ Нет доступа.")
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    topic_allowed = db.is_workspace_enabled(message.chat.id, thread_id, "topic") if thread_id else False
    group_allowed = db.is_workspace_enabled(message.chat.id, None, "group")
    allowed = topic_allowed or group_allowed
    logging.info("/esim workspace check chat_id=%s thread_id=%s topic_allowed=%s group_allowed=%s allowed=%s rows=%s", message.chat.id, thread_id, topic_allowed, group_allowed, allowed, debug_workspace_rows(message.chat.id))
    if not allowed:
        await message.answer("Эта группа или топик не включены как рабочая зона. Используй /work или /topic.")
        return
    await message.answer("<b>📥 Выбор номера ESIM</b>\n\nСначала выберите режим, который нужен:", reply_markup=esim_mode_kb(message.from_user.id))


@router.callback_query(F.data == "esim:back_mode")
async def esim_back_mode(callback: CallbackQuery):
    if not consume_event_once("cb_esim_back", callback.id):
        await safe_callback_answer(callback)
        return
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    text = "<b>📥 Выбор номера ESIM</b>\n\nСначала выберите режим, который нужен:"
    await safe_edit_or_send(callback, text, reply_markup=esim_mode_kb(callback.from_user.id))
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("esim_mode:"))
async def esim_choose_mode(callback: CallbackQuery):
    logging.info("esim_choose_mode callback=%s", callback.data)
    if not consume_event_once("cb_esim_mode", callback.id):
        await safe_callback_answer(callback)
        return
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    mode = callback.data.split(':', 1)[1]
    text = f"<b>📥 Выбор номера ESIM</b>\n\nВыбран режим: <b>{mode_label(mode)}</b>\n👇 Теперь выберите оператора:\n<i>Цена указана прямо в кнопках.</i>"
    thread_id = getattr(callback.message, 'message_thread_id', None)
    await safe_edit_or_send(callback, text, reply_markup=operators_group_kb(callback.message.chat.id, thread_id, mode, 'esim_take', 'esim:back_mode'))
    await safe_callback_answer(callback)


@router.callback_query(F.data.startswith("esim_take:"))
async def esim_take(callback: CallbackQuery):
    logging.info("esim_take callback=%s", callback.data)
    if not consume_event_once("cb_esim_take", callback.id):
        await safe_callback_answer(callback)
        return
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    _, operator_key, mode = callback.data.split(':')
    thread_id = getattr(callback.message, 'message_thread_id', None)
    topic_allowed = db.is_workspace_enabled(callback.message.chat.id, thread_id, 'topic') if thread_id else False
    group_allowed = db.is_workspace_enabled(callback.message.chat.id, None, 'group')
    allowed = topic_allowed or group_allowed
    logging.info("esim_take workspace check chat_id=%s thread_id=%s topic_allowed=%s group_allowed=%s allowed=%s rows=%s", callback.message.chat.id, thread_id, topic_allowed, group_allowed, allowed, debug_workspace_rows(callback.message.chat.id))
    if not allowed:
        await callback.answer('Рабочая зона не активирована', show_alert=True)
        return
    item = get_next_queue_item_mode(operator_key, mode)
    if not item:
        await callback.answer('В этой очереди пока пусто', show_alert=True)
        return
    if callback.message.chat.type == ChatType.PRIVATE:
        await callback.answer('Команда доступна только в группе', show_alert=True)
        return
    group_price = group_price_for_take(callback.message.chat.id, thread_id, item.operator_key, item.mode)
    if db.get_group_balance(callback.message.chat.id, thread_id) + 1e-9 < group_price:
        await callback.answer(f"Недостаточно средств в казне группы. Нужно {usd(group_price)}", show_alert=True)
        return
    if not db.reserve_queue_item_for_group(item.id, callback.from_user.id, callback.message.chat.id, thread_id, group_price):
        await callback.answer("Заявку уже забрали", show_alert=True)
        return
    fresh = db.get_queue_item(item.id)
    try:
        await send_queue_item_photo_to_chat(callback.bot, callback.message.chat.id, fresh, queue_caption(fresh), reply_markup=admin_queue_kb(fresh), message_thread_id=thread_id)
    except Exception:
        db.release_item_reservation(item.id)
        db.conn.execute("UPDATE queue_items SET status='queued', taken_by_admin=NULL, taken_at=NULL WHERE id=?", (item.id,))
        db.conn.commit()
        raise
    try:
        await send_item_user_message(
            callback.bot,
            fresh,
            f"<b>📥 Номер взят в обработку</b>\n\n🧾 <b>Заявка:</b> #{fresh.id}\n📱 <b>Оператор:</b> {op_html(fresh.operator_key)}\n📞 <b>Номер:</b> <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n🔄 <b>Режим:</b> {mode_label(fresh.mode)}"
        )
    except Exception:
        pass
    await callback.answer('Заявка выдана')


@router.callback_query(F.data.startswith("wd_delcheck:"))
async def wd_delcheck(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    wd_id = int(callback.data.split(':')[-1])
    wd = db.get_withdrawal(wd_id)
    if not wd or not wd['payout_check_id']:
        await callback.answer('Чек не найден', show_alert=True)
        return
    ok, note = await delete_crypto_check(int(wd['payout_check_id']))
    await callback.answer(note, show_alert=not ok)



async def mirror_polling_loop(bot: Bot):
    offset = 0
    while True:
        try:
            updates = await bot.get_updates(offset=offset, timeout=25, allowed_updates=["message", "callback_query"])
            for upd in updates:
                offset = upd.update_id + 1
                try:
                    await LIVE_DP.feed_update(bot, upd)
                except Exception:
                    logging.exception("mirror feed_update failed")
        except Exception:
            logging.exception("mirror polling loop failed")
            await asyncio.sleep(3)

async def start_live_mirror(token: str):
    global LIVE_DP
    token = (token or "").strip()
    if not token or token == BOT_TOKEN or token in LIVE_MIRROR_TASKS:
        return False, "already_started"
    if LIVE_DP is None:
        return False, "dispatcher_not_ready"
    try:
        mirror_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        me = await mirror_bot.get_me()
        task = asyncio.create_task(mirror_polling_loop(mirror_bot))
        LIVE_MIRROR_TASKS[token] = {"task": task, "username": me.username or "", "bot": mirror_bot}
        logging.info("Live mirror started as @%s", me.username or "unknown")
        return True, me.username or ""
    except Exception as e:
        logging.exception("Live mirror start failed: %s", e)
        return False, str(e)

async def hold_watcher(bot: Bot):
    while True:
        try:
            # update active hold captions every ~30 sec
            active_items = db.get_active_holds_for_render()
            for item in active_items:
                try:
                    if item.status != "in_progress":
                        continue
                    last = parse_dt(item.timer_last_render) if item.timer_last_render else None
                    now_dt = msk_now()
                    if last is None or (now_dt - last).total_seconds() >= 30:
                        await bot.edit_message_caption(
                            chat_id=item.work_chat_id,
                            message_id=item.work_message_id,
                            caption=queue_caption(item),
                            reply_markup=admin_queue_kb(item),
                        )
                        if getattr(item, 'user_hold_chat_id', None) and getattr(item, 'user_hold_message_id', None):
                            try:
                                await bot.edit_message_caption(
                                    chat_id=item.user_hold_chat_id,
                                    message_id=item.user_hold_message_id,
                                    caption=queue_caption(item),
                                    reply_markup=None,
                                )
                            except Exception:
                                pass
                        db.touch_timer_render(item.id)
                except Exception:
                    pass

            # complete expired holds
            expired_items = db.get_expired_holds()
            for item in expired_items:
                try:
                    db.complete_queue_item(item.id)
                    db.add_balance(item.user_id, float(item.price))
                    referrer_id, ref_bonus = credit_referral_bonus(item.user_id, queue_item_margin(item))
                    if referrer_id and ref_bonus > 0:
                        try:
                            await notify_user(bot, referrer_id, f"<b>🎁 Реферальное начисление</b>\n\nВаш реферал заработал {usd(item.price)}.\nВам начислено 5%: <b>{usd(ref_bonus)}</b>")
                        except Exception:
                            pass
                    fresh_user = db.get_user(item.user_id)
                    balance = float(fresh_user["balance"] if fresh_user else 0.0)
                    try:
                        await send_item_user_message(
                            bot,
                            item,
                            "<b>✅ Оплата за номер</b>\n\n"
                            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
                            f"💰 <b>Начислено:</b> {usd(item.price)}\n"
                            f"💲 <b>Ваш баланс:</b> {usd(balance)}"
                        )
                    except Exception:
                        pass
                    try:
                        final_item = db.get_queue_item(item.id) or item
                        await bot.edit_message_caption(
                            chat_id=item.work_chat_id,
                            message_id=item.work_message_id,
                            caption=queue_caption(final_item) + "\n\n✅ <b>Холд завершён. Номер оплачен.</b>",
                            reply_markup=None,
                        )
                        if getattr(item, 'user_hold_chat_id', None) and getattr(item, 'user_hold_message_id', None):
                            try:
                                await bot.edit_message_caption(
                                    chat_id=item.user_hold_chat_id,
                                    message_id=item.user_hold_message_id,
                                    caption=queue_caption(final_item) + "\n\n✅ <b>Холд завершён. Номер оплачен.</b>",
                                    reply_markup=None,
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            logging.exception("hold_watcher failed")
        await asyncio.sleep(5)


def render_admin_queue_text() -> str:
    items = latest_queue_items(10)
    if not items:
        return "<b>📦 Очередь</b>\n\n<i>Активных заявок в очереди нет.</i>"
    rows = []
    for item in items:
        pos = queue_position(item['id']) if item['status'] == 'queued' else None
        pos_text = f" • позиция {pos}" if pos else ""
        priority_text = ""
        rows.append(f"#{item['id']} • {op_text(item['operator_key'])} • {mode_label(item['mode'])} • {pretty_phone(item['normalized_phone'])}{priority_text}{pos_text}")
    return "<b>📦 Очередь</b>\n\n" + quote_block(rows)

@router.callback_query(F.data == "admin:queues")
async def admin_queues(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, render_admin_queue_text(), reply_markup=queue_manage_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data == "admin:user_tools")
async def admin_user_tools(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await safe_edit_or_send(
        callback,
        "<b>👤 Пользователь</b>\n\nВыберите действие ниже, затем отправьте ID, @username или номер следующим сообщением.",
        reply_markup=user_admin_kb(),
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data.in_(["admin:user_stats", "admin:user_set_price", "admin:user_pm", "admin:user_add_balance", "admin:user_sub_balance", "admin:user_ban", "admin:user_unban"]))
async def admin_user_action_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    action_map = {
        "admin:user_stats": "stats",
        "admin:user_set_price": "set_price",
        "admin:user_pm": "pm",
        "admin:user_add_balance": "add_balance",
        "admin:user_sub_balance": "sub_balance",
        "admin:user_ban": "ban",
        "admin:user_unban": "unban",
    }
    action = action_map.get(callback.data, "")
    await state.clear()
    await state.update_data(user_action=action)
    await state.set_state(AdminStates.waiting_user_action_id)
    prompts = {
        "stats": "<b>Отправьте ID, @username или номер пользователя для просмотра статистики:</b>",
        "set_price": "<b>Отправьте ID, @username или номер пользователя для персонального прайса:</b>",
        "pm": "<b>Отправьте ID, @username или номер пользователя для сообщения в ЛС:</b>",
        "add_balance": "<b>Отправьте ID, @username или номер пользователя для начисления:</b>",
        "sub_balance": "<b>Отправьте ID, @username или номер пользователя для списания:</b>",
        "ban": "<b>Отправьте ID, @username или номер пользователя для блокировки:</b>",
        "unban": "<b>Отправьте ID, @username или номер пользователя для разблокировки:</b>",
    }
    await callback.message.answer(prompts.get(action, "<b>Отправьте ID, @username или номер пользователя:</b>"))
    await safe_callback_answer(callback)

@router.message(AdminStates.waiting_user_action_id)
async def admin_user_action_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    action = data.get("user_action")
    raw = (message.text or "").strip()
    logging.info("user-section lookup action=%s raw=%s", action, raw)
    user = resolve_user_input(raw)

    if not user:
        await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или номер ещё раз.")
        return

    target_user_id = int(user["user_id"])
    await state.update_data(target_user_id=target_user_id)
    logging.info("user-section found target_user_id=%s action=%s", target_user_id, action)

    if action == "stats":
        full_user, stats, ops = get_user_full_stats(target_user_id)
        ops_text = "\n".join(
            f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}"
            for row in ops
        ) or "• Пока пусто"
        custom_prices = db.list_user_prices(target_user_id) if hasattr(db, "list_user_prices") else []
        custom_text = "\n".join(
            f"• {op_text(row['operator_key'])} • {mode_label(row['mode'])} = <b>{usd(row['price'])}</b>"
            for row in custom_prices
        ) or "• Нет"
        await state.clear()
        await message.answer(
            f"<b>👤 Пользователь</b>\n\n"
            f"🆔 <code>{target_user_id}</code>\n"
            f"👤 <b>{escape(full_user['full_name'] or '')}</b>\n"
            f"🔗 @{escape(full_user['username']) if full_user['username'] else '—'}\n"
            f"💰 Баланс: <b>{usd(full_user['balance'])}</b>\n\n"
            f"📊 Всего: <b>{stats['total'] or 0}</b> | ✅ <b>{stats['completed'] or 0}</b> | ❌ <b>{stats['slipped'] or 0}</b> | ⚠️ <b>{stats['errors'] or 0}</b>\n"
            f"💵 Заработано: <b>{usd(stats['earned'] or 0)}</b>\n\n"
            f"<b>📱 По операторам</b>\n{ops_text}\n\n"
            f"<b>💎 Персональные прайсы</b>\n{custom_text}",
            reply_markup=admin_back_kb("admin:user_tools"),
        )
        return

    if action == "set_price":
        await state.set_state(AdminStates.waiting_user_price_lookup)
        await message.answer(
            "<b>✅ Пользователь найден</b>\n\n"
            f"👤 <b>{escape(user['full_name'] or '')}</b>\n"
            f"🆔 <code>{target_user_id}</code>\n"
            f"🔗 @{escape(user['username']) if user['username'] else '—'}\n\n"
            "<b>Выберите оператора:</b>",
            reply_markup=user_price_operator_kb(target_user_id),
        )
        return

    if action in {"add_balance", "sub_balance"}:
        await state.set_state(AdminStates.waiting_user_action_value)
        await message.answer("Введите сумму в $:")
        return

    if action == "pm":
        await state.set_state(AdminStates.waiting_user_action_text)
        await message.answer("Введите текст сообщения для пользователя:")
        return

    if action == "ban":
        set_user_blocked(target_user_id, True)
        await state.clear()
        await message.answer(f"✅ Пользователь <code>{target_user_id}</code> заблокирован.", reply_markup=admin_back_kb("admin:user_tools"))
        return

    if action == "unban":
        set_user_blocked(target_user_id, False)
        await state.clear()
        await message.answer(f"✅ Пользователь <code>{target_user_id}</code> разблокирован.", reply_markup=admin_back_kb("admin:user_tools"))
        return

@router.message(AdminStates.waiting_user_price_lookup)
async def admin_user_price_lookup(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    raw = (message.text or "").strip()
    if raw:
        user = resolve_user_input(raw)
        if user:
            target_user_id = int(user["user_id"])
            await state.update_data(target_user_id=target_user_id)
        else:
            await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или номер ещё раз.")
            return

    data = await state.get_data()
    target_user_id = int(data["target_user_id"])
    await message.answer("<b>Выберите оператора:</b>", reply_markup=user_price_operator_kb(target_user_id))

@router.callback_query(F.data.startswith("admin:user_price_op:"))
async def admin_user_price_op(callback: CallbackQuery):
    logging.info("admin_user_price_op callback=%s", callback.data)
    if not is_admin(callback.from_user.id):
        return
    await safe_callback_answer(callback)
    _, _, uid, operator_key = callback.data.split(":")
    await callback.message.answer(
        f"<b>Пользователь:</b> <code>{uid}</code>\n<b>Оператор:</b> {op_text(operator_key)}\n\n<b>Выберите режим:</b>",
        reply_markup=user_price_mode_kb(int(uid), operator_key),
    )

@router.callback_query(F.data.startswith("admin:user_price_mode:"))
async def admin_user_price_mode(callback: CallbackQuery, state: FSMContext):
    logging.info("admin_user_price_mode callback=%s", callback.data)
    if not is_admin(callback.from_user.id):
        return
    await safe_callback_answer(callback)
    _, _, uid, operator_key, mode = callback.data.split(":")
    await state.set_state(AdminStates.waiting_user_price_value)
    await state.update_data(target_user_id=int(uid), operator_key=operator_key, price_mode=mode)
    await callback.message.answer(
        f"<b>Пользователь:</b> <code>{uid}</code>\n"
        f"<b>Оператор:</b> {op_text(operator_key)}\n"
        f"<b>Режим:</b> {mode_label(mode)}\n\n"
        "Введите сумму числом или <code>reset</code> для удаления:",
        reply_markup=admin_back_kb("admin:user_tools"),
    )

@router.message(AdminStates.waiting_user_price_value)
async def admin_user_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    operator_key = data["operator_key"]
    mode = data["price_mode"]
    value_raw = (message.text or "").strip().lower()

    if value_raw in {"reset", "delete", "del", "none"}:
        if hasattr(db, "delete_user_price"):
            db.delete_user_price(uid, operator_key, mode)
        await state.clear()
        await message.answer(
            f"✅ Персональный прайс удалён\n\n"
            f"👤 Пользователь: <code>{uid}</code>\n"
            f"📱 Оператор: {op_text(operator_key)}\n"
            f"🔄 Режим: <b>{mode_label(mode)}</b>",
            reply_markup=admin_back_kb("admin:user_tools"),
        )
        return

    try:
        value = float(value_raw.replace(",", "."))
    except Exception:
        await message.answer("⚠️ Введите сумму числом или <code>reset</code>.")
        return

    db.set_user_price(uid, operator_key, mode, value)
    await state.clear()
    await message.answer(
        f"✅ Персональный прайс сохранён\n\n"
        f"👤 Пользователь: <code>{uid}</code>\n"
        f"📱 Оператор: {op_text(operator_key)}\n"
        f"🔄 Режим: <b>{mode_label(mode)}</b>\n"
        f"💰 Цена: <b>{usd(value)}</b>",
        reply_markup=admin_back_kb("admin:user_tools"),
    )

@router.message(AdminStates.waiting_user_action_value)
async def admin_user_action_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    action = data.get("user_action")
    try:
        value = float((message.text or "").replace(",", "."))
    except Exception:
        await message.answer("Введите сумму числом.")
        return

    if action == "add_balance":
        db.add_balance(uid, value)
        await state.clear()
        await message.answer(f"✅ Пользователю <code>{uid}</code> начислено <b>{usd(value)}</b>.", reply_markup=admin_back_kb("admin:user_tools"))
        return

    if action == "sub_balance":
        db.subtract_balance(uid, value)
        await state.clear()
        await message.answer(f"✅ У пользователя <code>{uid}</code> списано <b>{usd(value)}</b>.", reply_markup=admin_back_kb("admin:user_tools"))
        return

@router.message(AdminStates.waiting_user_action_text)
async def admin_user_action_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    try:
        await message.bot.send_message(uid, f"<b>📩 Сообщение от администрации</b>\n\n{escape(message.text)}")
        await message.answer("✅ Сообщение отправлено.", reply_markup=admin_back_kb("admin:user_tools"))
    except Exception:
        await message.answer("⚠️ Не удалось отправить сообщение.", reply_markup=admin_back_kb("admin:user_tools"))
    await state.clear()

@router.callback_query(F.data == "admin:toggle_numbers")
async def admin_toggle_numbers(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    set_numbers_enabled(not is_numbers_enabled())
    await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
    await callback.answer("Статус обновлён")

@router.callback_query(F.data.startswith("admin:queue_remove:"))
async def admin_queue_remove(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    item_id = int(callback.data.split(":")[-1])
    remove_queue_item(item_id, reason='admin_removed', admin_id=callback.from_user.id)
    await safe_edit_or_send(callback, render_admin_queue_text(), reply_markup=queue_manage_kb())
    await callback.answer("Удалено из очереди")

@router.callback_query(F.data.startswith("myremove:"))
async def myremove_cb(callback: CallbackQuery, state: FSMContext):
    parts = (callback.data or "").split(":")
    try:
        item_id = int(parts[1])
    except Exception:
        await callback.answer("Некорректная кнопка удаления", show_alert=True)
        return
    try:
        page = int(parts[2]) if len(parts) > 2 else 0
    except Exception:
        page = 0
    row = db.conn.execute(
        "SELECT * FROM queue_items WHERE id=? AND user_id=? AND status IN ('queued','taken','in_progress')",
        (item_id, callback.from_user.id),
    ).fetchone()
    if not row:
        items = user_active_queue_items(callback.from_user.id)
        await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id, page), my_numbers_kb(items, page))
        await callback.answer("Заявка уже неактуальна или обработана", show_alert=True)
        return
    if row["status"] != "queued":
        await callback.answer("Убрать можно только номер из очереди", show_alert=True)
        return
    remove_queue_item(item_id, reason='user_removed')
    items = user_active_queue_items(callback.from_user.id)
    await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id, page), my_numbers_kb(items, page))
    await send_log(callback.bot, f"<b>🗑 Удаление из очереди</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>")
    await callback.answer("Номер убран")

@router.callback_query(F.data.startswith("take_start:"))
async def take_start_cb(callback: CallbackQuery):
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item or item.status not in {"queued", "taken"}:
        await callback.answer("Заявка уже неактуальна", show_alert=True)
        return
    thread_id = getattr(callback.message, 'message_thread_id', None)
    db.start_work(item.id, callback.from_user.id, item.mode, callback.message.chat.id, thread_id, callback.message.message_id)
    fresh = db.get_queue_item(item.id)
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=queue_caption(fresh), reply_markup=admin_queue_kb(fresh))
        else:
            await callback.message.edit_text(queue_caption(fresh), reply_markup=admin_queue_kb(fresh))
    except Exception:
        pass
    try:
        if fresh.mode == 'hold':
            user_msg = await send_queue_item_photo_to_chat(callback.bot, int(fresh.user_id), fresh, queue_caption(fresh, price_view='submit'), message_thread_id=None)
            if user_msg:
                db.conn.execute("UPDATE queue_items SET user_hold_chat_id=?, user_hold_message_id=? WHERE id=?", (int(fresh.user_id), int(user_msg.message_id), fresh.id))
                db.conn.commit()
        else:
            await send_item_user_message(
                callback.bot,
                fresh,
                "<b>✅ Номер — Встал ✅</b>\n\n"
                "🚀 <b>По вашему номеру началась работа</b>\n\n"
                f"📞 <b>Номер:</b> <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n"
                f"📱 <b>Оператор:</b> {op_html(fresh.operator_key)}\n"
                f"{mode_emoji(fresh.mode)} <b>Режим:</b> {mode_label(fresh.mode)}\n"
                f"🏷 <b>Прайс сдачи:</b> {usd(float(fresh.price))}"
            )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>🚀 Работа началась</b>\n👤 Взял: {escape(callback.from_user.full_name)}\n🆔 <code>{callback.from_user.id}</code>\n🧾 Заявка: <b>#{fresh.id}</b>\n📱 {op_html(fresh.operator_key)}\n📞 <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n🔄 {mode_label(fresh.mode)}")
    await callback.answer("Работа началась")

@router.callback_query(F.data.startswith("error_pre:"))
async def error_pre_cb(callback: CallbackQuery):
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item:
        await callback.answer("Заявка не найдена", show_alert=True)
        return
    db.mark_error_before_start(item_id)
    db.release_item_reservation(item_id)
    fresh = db.get_queue_item(item_id) or item
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=queue_caption(fresh) + "\n\n⚠️ <b>Ошибка — номер не встал.</b>", reply_markup=None)
        else:
            await callback.message.edit_text(queue_caption(fresh) + "\n\n⚠️ <b>Ошибка — номер не встал.</b>", reply_markup=None)
    except Exception:
        pass
    try:
        await send_item_user_message(
            callback.bot,
            item,
            "<b>⚠️ Ошибка — номер не встал</b>\n\n"
            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
            "❌ <b>Номер не принят в работу.</b>"
        )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>⚠️ Ошибка заявки</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>\n📱 {op_html(item.operator_key)}")
    await callback.answer("Помечено как ошибка")

@router.callback_query(F.data.startswith("instant_pay:"))
async def instant_pay_cb(callback: CallbackQuery):
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item or item.status != "in_progress" or item.mode != "no_hold":
        await callback.answer("Оплата недоступна", show_alert=True)
        return
    db.complete_queue_item(item_id)
    db.add_balance(item.user_id, float(item.price))
    referrer_id, ref_bonus = credit_referral_bonus(item.user_id, queue_item_margin(item))
    if referrer_id and ref_bonus > 0:
        try:
            await notify_user(callback.bot, referrer_id, f"<b>🎁 Реферальное начисление</b>\n\nВаш реферал заработал {usd(item.price)}.\nВам начислено 5%: <b>{usd(ref_bonus)}</b>")
        except Exception:
            pass
    user = db.get_user(item.user_id)
    balance = float(user["balance"] if user else 0)
    fresh = db.get_queue_item(item_id) or item
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=queue_caption(fresh) + "\n\n✅ <b>Оплачено.</b>", reply_markup=None)
        else:
            await callback.message.edit_text(queue_caption(fresh) + "\n\n✅ <b>Оплачено.</b>", reply_markup=None)
    except Exception:
        pass
    try:
        await send_item_user_message(
            callback.bot,
            item,
            "<b>✅ Оплата за номер</b>\n\n"
            f"📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
            f"💰 <b>Начислено:</b> {usd(item.price)}\n"
            f"💲 <b>Ваш баланс:</b> {usd(balance)}"
        )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>💸 Оплата номера</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>\n📱 {op_html(item.operator_key)}\n💰 {usd(item.price)}")
    await callback.answer("Оплачено")

@router.callback_query(F.data.startswith("slip:"))
async def slip_cb(callback: CallbackQuery):
    allowed_actor, actor_reason = await callback_actor_can_take_esim(callback)
    logging.info("%s actor check chat_id=%s user_id=%s allowed=%s reason=%s", callback.data, callback.message.chat.id, callback.from_user.id, allowed_actor, actor_reason)
    if not allowed_actor:
        await safe_callback_answer(callback, "⛔ Нет доступа", show_alert=True)
        return
    item_id = int(callback.data.split(":")[-1])
    item = db.get_queue_item(item_id)
    if not item or item.status != "in_progress":
        await callback.answer("Слет недоступен", show_alert=True)
        return
    started = parse_dt(item.work_started_at)
    worked = "00:00"
    if started:
        secs = max(int((msk_now() - started).total_seconds()), 0)
        worked = f"{secs//60:02d}:{secs%60:02d}"
    db.conn.execute("UPDATE queue_items SET status='failed', fail_reason='slip', completed_at=? WHERE id=?", (now_str(), item_id))
    db.conn.commit()
    db.release_item_reservation(item_id)
    fresh = db.get_queue_item(item_id) or item
    remain = time_left_text(item.hold_until) if item.mode == "hold" else "—"
    slip_text = queue_caption(fresh) + f"\n\n❌ <b>Номер слетел</b>\n⏱ <b>Время работы:</b> {worked}\n▫️ <b>Холд осталось:</b> {remain}\n\n❌ <b>Оплата за номер не начислена.</b>"
    try:
        if getattr(callback.message, "photo", None):
            await callback.message.edit_caption(caption=slip_text, reply_markup=None)
        else:
            await callback.message.edit_text(slip_text, reply_markup=None)
    except Exception:
        pass
    try:
        if getattr(item, 'user_hold_chat_id', None) and getattr(item, 'user_hold_message_id', None):
            try:
                await callback.bot.edit_message_caption(
                    chat_id=item.user_hold_chat_id,
                    message_id=item.user_hold_message_id,
                    caption=user_slip_text,
                    reply_markup=None,
                )
            except Exception:
                pass
        else:
            await send_item_user_message(
                callback.bot,
                item,
                f"<b>❌ Номер слетел</b>\n\n📞 <b>Номер:</b> <code>{escape(pretty_phone(item.normalized_phone))}</code>\n⏱ <b>Время работы:</b> {worked}\n▫️ <b>Холд осталось:</b> {remain}\n\n❌ <b>Оплата за номер не начислена.</b>"
            )
    except Exception:
        pass
    await send_log(callback.bot, f"<b>❌ Слет</b>\n👤 {escape(callback.from_user.full_name)}\n🧾 Заявка: <b>#{item_id}</b>\n📱 {op_html(item.operator_key)}")
    await callback.answer("Слет отмечен")

@router.callback_query(F.data.in_(["admin:user_stats", "admin:user_set_price", "admin:user_pm", "admin:user_add_balance", "admin:user_sub_balance", "admin:user_ban", "admin:user_unban"]))
async def admin_user_action_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    raw_action = callback.data.split(":")[-1]
    action_map = {
        "user_stats": "stats",
        "user_set_price": "set_price",
        "user_pm": "pm",
        "user_add_balance": "add_balance",
        "user_sub_balance": "sub_balance",
        "user_ban": "ban",
        "user_unban": "unban",
    }
    action = action_map.get(raw_action, raw_action)
    await state.clear()
    if action == "stats":
        await state.set_state(AdminStates.waiting_user_stats_lookup)
        await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя:</b>", reply_markup=ForceReply(selective=True))
        await safe_callback_answer(callback)
        return
    if action == "set_price":
        await state.set_state(AdminStates.waiting_user_price_lookup)
        await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя для персонального прайса:</b>", reply_markup=ForceReply(selective=True))
        await safe_callback_answer(callback)
        return
    await state.update_data(user_action=action)
    await state.set_state(AdminStates.waiting_user_action_id)
    await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя:</b>", reply_markup=ForceReply(selective=True))
    await safe_callback_answer(callback)

@router.message(AdminStates.waiting_user_stats_lookup)
async def admin_user_stats_lookup(message: Message, state: FSMContext):
    logging.info("admin_user_stats_lookup: %s", message.text)
    logging.info("user-section handler: stats | text=%s | user=%s", getattr(message if 'stats' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'stats' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    user = resolve_user_input(message.text)
    if not user:
        await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или сданный номер ещё раз.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return
    target_user_id = int(user["user_id"])
    user, stats, ops = get_user_full_stats(target_user_id)
    if not user:
        await message.answer("⚠️ Пользователь не найден. Попробуйте ещё раз.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return
    ops_text = "\n".join([f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}" for row in ops]) or "• Пока пусто"
    custom_prices = db.list_user_prices(target_user_id)
    custom_text = "\n".join(
        f"• {op_text(row['operator_key'])} • {mode_label(row['mode'])} = <b>{usd(row['price'])}</b>"
        for row in custom_prices
    ) or "• Нет"
    text_msg = (
        f"<b>👤 Пользователь</b>\n\n"
        f"🆔 <code>{target_user_id}</code>\n"
        f"👤 <b>{escape(user['full_name'] or '')}</b>\n"
        f"🔗 @{escape(user['username']) if user['username'] else '—'}\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"📊 Всего: <b>{stats['total'] or 0}</b> | ✅ <b>{stats['completed'] or 0}</b> | ❌ <b>{stats['slipped'] or 0}</b> | ⚠️ <b>{stats['errors'] or 0}</b>\n"
        f"💵 Заработано: <b>{usd(stats['earned'] or 0)}</b>\n\n"
        f"<b>📱 По операторам</b>\n{ops_text}\n\n"
        f"<b>💎 Персональные прайсы</b>\n{custom_text}"
    )
    await state.clear()
    await message.answer(text_msg, reply_markup=admin_back_kb("admin:user_tools"))

@router.message(AdminStates.waiting_user_price_lookup)
async def admin_user_price_lookup(message: Message, state: FSMContext):
    logging.info("admin_user_price_lookup: %s", message.text)
    logging.info("user-section handler: lookup | text=%s | user=%s", getattr(message if 'lookup' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'lookup' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    user = resolve_user_input(raw)
    if not user:
        await message.answer("⚠️ Пользователь не найден. Отправьте ID, @username или сданный номер ещё раз.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return
    uid = int(user["user_id"])
    await state.clear()
    await message.answer(
        "<b>✅ Пользователь найден</b>\n\n"
        f"👤 <b>{escape(user['full_name'] or '')}</b>\n"
        f"🆔 <code>{uid}</code>\n"
        f"🔗 @{escape(user['username']) if user['username'] else '—'}\n\n"
        "<b>Выберите оператора:</b>",
        reply_markup=user_price_operator_kb(uid),
    )

@router.callback_query(F.data.startswith("admin:user_price_back_ops:"))
async def admin_user_price_back_ops(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    uid = int(callback.data.split(":")[-1])
    await safe_edit_or_send(callback, "<b>Выберите оператора:</b>", reply_markup=user_price_operator_kb(uid))
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:user_price_op:"))
async def admin_user_price_op(callback: CallbackQuery):
    logging.info("admin_user_price_op: %s", callback.data)
    logging.info("user-section handler: op | text=%s | user=%s", getattr(message if 'op' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'op' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(callback.from_user.id):
        return
    _, _, uid, operator_key = callback.data.split(":")
    await safe_edit_or_send(
        callback,
        f"<b>Пользователь:</b> <code>{uid}</code>\n<b>Оператор:</b> {op_text(operator_key)}\n\n<b>Выберите режим:</b>",
        reply_markup=user_price_mode_kb(int(uid), operator_key),
    )
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:user_price_mode:"))
async def admin_user_price_mode(callback: CallbackQuery, state: FSMContext):
    logging.info("admin_user_price_mode: %s", callback.data)
    logging.info("user-section handler: mode | text=%s | user=%s", getattr(message if 'mode' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'mode' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(callback.from_user.id):
        return
    _, _, uid, operator_key, mode = callback.data.split(":")
    await state.set_state(AdminStates.waiting_user_price_value)
    await state.update_data(target_user_id=int(uid), operator_key=operator_key, price_mode=mode)
    await callback.message.answer(
        f"<b>Пользователь:</b> <code>{uid}</code>\n"
        f"<b>Оператор:</b> {op_text(operator_key)}\n"
        f"<b>Режим:</b> {mode_label(mode)}\n\n"
        "Введите сумму числом.\nЧтобы удалить персональный прайс, отправьте: <code>reset</code>",
        reply_markup=cancel_inline_kb("admin:user_tools"),
    )
    await safe_callback_answer(callback)

@router.message(AdminStates.waiting_user_price_value)
async def admin_user_price_value(message: Message, state: FSMContext):
    logging.info("admin_user_price_value: %s", message.text)
    logging.info("user-section handler: value | text=%s | user=%s", getattr(message if 'value' not in ["op","mode"] else callback, "text", None) if False else None, (message.from_user.id if 'value' not in ["op","mode"] else callback.from_user.id))
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    operator_key = data["operator_key"]
    mode = data["price_mode"]
    value_raw = (message.text or "").strip().lower()

    if value_raw in {"reset", "delete", "del", "none"}:
        db.delete_user_price(uid, operator_key, mode)
        await state.clear()
        await message.answer(
            f"✅ Персональный прайс удалён\n\n"
            f"👤 Пользователь: <code>{uid}</code>\n"
            f"📱 Оператор: {op_text(operator_key)}\n"
            f"🔄 Режим: <b>{mode_label(mode)}</b>",
            reply_markup=admin_back_kb("admin:user_tools"),
        )
        return

    try:
        value = float(value_raw.replace(",", "."))
    except Exception:
        await message.answer("⚠️ Введите сумму числом или <code>reset</code>.", reply_markup=cancel_inline_kb("admin:user_tools"))
        return

    db.set_user_price(uid, operator_key, mode, value)
    await state.clear()
    await message.answer(
        f"✅ Персональный прайс сохранён\n\n"
        f"👤 Пользователь: <code>{uid}</code>\n"
        f"📱 Оператор: {op_text(operator_key)}\n"
        f"🔄 Режим: <b>{mode_label(mode)}</b>\n"
        f"💰 Цена: <b>{usd(value)}</b>",
        reply_markup=admin_back_kb("admin:user_tools"),
    )

@router.message(AdminStates.waiting_user_custom_price_text)
async def admin_user_custom_price_text_legacy(message: Message, state: FSMContext):
    await state.set_state(AdminStates.waiting_user_price_value)
    await admin_user_price_value(message, state)

@router.message(AdminStates.waiting_user_action_text)
async def admin_user_action_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = int(data["target_user_id"])
    try:
        await message.bot.send_message(uid, f"<b>📩 Сообщение от администрации</b>\n\n{escape(message.text)}")
        await message.answer("Сообщение отправлено.")
    except Exception:
        await message.answer("Не удалось отправить сообщение.")
    await state.clear()


@router.message(Command("dbsqulite"))
async def db_sqlite_export(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = Path(DB_PATH)
    if not path.exists():
        await message.answer("Файл базы пока не найден.")
        return
    await message.answer_document(FSInputFile(path), caption="<b>📦 SQLite база</b>")

@router.message(Command("dblog"))
async def db_log_export(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = Path("bot.log")
    if not path.exists():
        path.write_text("Лог пока пуст.\n", encoding="utf-8")
    await message.answer_document(FSInputFile(path), caption="<b>🧾 Логи бота</b>")

@router.message(Command("dbusernames"))
async def export_usernames_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    data = db.export_usernames().strip() or "Нет username."
    path = Path("usernames.txt")
    path.write_text(data + ("\n" if not data.endswith("\n") else ""), encoding="utf-8")
    await message.answer_document(FSInputFile(path), caption="<b>👥 Username пользователей</b>")

@router.message(Command("uploadsqlite"))
@router.message(Command("dbupload"))
async def db_upload_command(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.waiting_db_upload)
    await message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")

@router.message(AdminStates.waiting_db_upload, F.document)
async def db_upload_receive(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    doc = message.document
    name = (doc.file_name or "").lower()
    if not (name.endswith(".db") or name.endswith(".sqlite") or name.endswith(".sqlite3")):
        await message.answer("Пришлите именно файл базы <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")
        return
    temp_path = Path(DB_PATH + ".uploaded")
    temp_path.unlink(missing_ok=True)
    await bot.download(doc, destination=temp_path)
    try:
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(str(temp_path))
        conn.execute("PRAGMA integrity_check").fetchone()
        conn.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
        conn.close()
    except Exception:
        temp_path.unlink(missing_ok=True)
        await message.answer("❌ Файл не похож на SQLite базу.")
        return
    try:
        backup_path = db.replace_with_uploaded_db(str(temp_path))
        ensure_extra_schema()
        restore_operators_from_db_anywhere()
        logging.info('DB upload post-restore operators: %s', sorted(OPERATORS.keys()))
        cleanup_database_size(18)
    except Exception:
        logging.exception("db_upload_receive failed")
        temp_path.unlink(missing_ok=True)
        await message.answer("❌ Не удалось загрузить базу. Посмотрите лог бота.")
        return
    await state.clear()
    await message.answer(
        "<b>✅ База загружена</b>\n\n"
        f"Текущая база заменена сразу. Резервная копия: <code>{escape(str(backup_path))}</code>"
    )

@router.message(AdminStates.waiting_db_upload)
async def db_upload_wrong(message: Message):
    await message.answer("Пришлите файл базы <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")


@router.message(F.text.regexp(r"^/(stata|stats)(?:@\w+)?$"))
@router.message(Command("stata"))
@router.message(Command("stats"))
@router.message(Command("Stata"))
async def group_stata(message: Message):
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Статистику групп смотрите через кнопку в /admin.")
        return
    try:
        member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
        if str(getattr(member, "status", "unknown")) not in {"creator", "administrator", "member", "restricted"}:
            await message.answer("⛔ Нет доступа.")
            return
    except Exception:
        logging.exception("group_stata get_chat_member failed chat_id=%s user_id=%s", message.chat.id, getattr(message.from_user, "id", None))
    try:
        day_start, day_end, day_label = msk_today_bounds_str()
        chat_id = message.chat.id
        thread_id = getattr(message, "message_thread_id", None)
        thread_key = db._thread_key(thread_id)
        active_sql = "'queued','taken','in_progress','waiting_check','checking','on_hold'"

        totals = db.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN taken_at IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
                SUM(CASE WHEN work_started_at IS NOT NULL THEN 1 ELSE 0 END) AS started,
                SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors,
                SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN status='completed' THEN price ELSE 0 END) AS paid_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) ELSE 0 END) AS spent_total,
                SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total,
                SUM(COALESCE(charge_amount, price)) AS turnover_total
            FROM queue_items
            WHERE charge_chat_id=? AND charge_thread_id=? AND ((taken_at>=? AND taken_at<?) OR status IN ({active_sql}))
            """,
            (int(chat_id), thread_key, day_start, day_end),
        ).fetchone()

        per_operator = db.conn.execute(
            """
            SELECT
                operator_key,
                COUNT(*) AS total,
                SUM(CASE WHEN mode='hold' THEN 1 ELSE 0 END) AS hold_total,
                SUM(CASE WHEN mode='no_hold' THEN 1 ELSE 0 END) AS no_hold_total,
                SUM(COALESCE(charge_amount, price)) AS turnover_total
            FROM queue_items
            WHERE charge_chat_id=? AND charge_thread_id=? AND ((taken_at>=? AND taken_at<?) OR status IN ({active_sql}))
            GROUP BY operator_key
            ORDER BY total DESC, operator_key ASC
            """,
            (int(chat_id), thread_key, day_start, day_end),
        ).fetchall()

        lines = [
            f"<b>📊 Статистика этой группы / топика за сегодня</b>",
            f"🗓 День: <b>{day_label}</b>",
            f"♻️ {msk_stats_reset_note()}",
            "",
            f"📦 Взято всего: <b>{int(totals['taken_total'] or 0)}</b>",
            f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>",
            f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>",
            f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>",
            f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>",
            f"🏦 Оборот группы: <b>{usd(totals['turnover_total'] or 0)}</b>",
        ]
        if per_operator:
            lines.append("")
            lines.append("<b>📱 По операторам</b>")
            for row in per_operator:
                lines.append(
                    f"• {op_text(row['operator_key'])}: <b>{int(row['total'] or 0)}</b> "
                    f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)}) • "
                    f"на сумму <b>{usd(row['turnover_total'] or 0)}</b>"
                )
        await message.answer("\n".join(lines))
    except Exception:
        logging.exception("group_stata failed chat_id=%s user_id=%s", message.chat.id, message.from_user.id)
        await message.answer("❌ Не удалось открыть статистику. Попробуйте ещё раз через /stats.")

@router.message(AdminStates.waiting_required_join_item)
async def admin_required_join_item_value(message: Message, state: FSMContext):
    if user_role(message.from_user.id) != "chief_admin":
        await state.clear()
        return
    raw = (message.text or '').strip()
    if raw == '-':
        await state.clear()
        await message.answer('Отменено.')
        return
    parts = [part.strip() for part in raw.split('|')]
    if not parts or not parts[0].lstrip('-').isdigit():
        await message.answer('Нужен формат: <code>-100xxxxxxxxxx | https://t.me/link | Название</code>')
        return
    chat_id = int(parts[0])
    link = parts[1] if len(parts) > 1 else ''
    title = parts[2] if len(parts) > 2 else ''
    items = required_join_entries()
    replaced = False
    for item in items:
        if int(item['chat_id']) == chat_id:
            item['link'] = link or item.get('link', '')
            item['title'] = title or item.get('title', '')
            replaced = True
            break
    if not replaced:
        items.append({'chat_id': chat_id, 'link': link, 'title': title})
    save_required_join_entries(items)
    await state.clear()
    await message.answer('✅ Канал обязательной подписки сохранён.')

@router.message(AdminStates.waiting_required_join_remove)
async def admin_required_join_remove_value(message: Message, state: FSMContext):
    if user_role(message.from_user.id) != "chief_admin":
        await state.clear()
        return
    raw = (message.text or '').strip()
    items = required_join_entries()
    target_chat_id = None
    if raw.isdigit():
        idx = int(raw)
        if 1 <= idx <= len(items):
            target_chat_id = int(items[idx - 1]['chat_id'])
    if target_chat_id is None and raw.lstrip('-').isdigit():
        target_chat_id = int(raw)
    if target_chat_id is None:
        await message.answer('Отправь номер из списка или ID канала.')
        return
    new_items = [item for item in items if int(item['chat_id']) != target_chat_id]
    save_required_join_entries(new_items)
    await state.clear()
    await message.answer('✅ Канал убран из обязательной подписки.')

@router.message(AdminStates.waiting_required_join_link)
async def admin_required_join_link_value(message: Message, state: FSMContext):
    if user_role(message.from_user.id) != "chief_admin":
        await state.clear()
        return
    raw = (message.text or '').strip()
    items = required_join_entries()
    if not items:
        db.set_setting('required_join_link', '' if raw == '-' else raw)
    else:
        items[0]['link'] = '' if raw == '-' else raw
        save_required_join_entries(items)
    await state.clear()
    await message.answer('✅ Ссылка сохранена.')

@router.message(AdminStates.waiting_channel_value)
async def admin_channel_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    if raw in {"0", "-", "clear", "off", "none"}:
        raw = "0"
    elif not raw.lstrip("-").isdigit():
        await message.answer("Введите ID канала числом. Для очистки отправьте <code>0</code>.")
        return
    data = await state.get_data()
    key = data.get("channel_target")
    if key not in {"withdraw_channel_id", "withdraw_thread_id", "log_channel_id", "backup_channel_id"}:
        await state.clear()
        await message.answer("❌ Неизвестная настройка.")
        return
    db.set_setting(key, raw)
    if key == "backup_channel_id" and raw == "0":
        db.set_setting("backup_enabled", "0")
    await state.clear()
    pretty = {
        "withdraw_channel_id": "Канал выплат",
        "withdraw_thread_id": "Топик выплат",
        "log_channel_id": "Канал логов",
        "backup_channel_id": "Канал автобэкапа",
    }.get(key, key)
    await message.answer(f"✅ <b>{pretty}</b> сохранён: <code>{escape(raw)}</code>", reply_markup=admin_back_kb())




@router.message(Command("kazna"))
async def kazna_command(message: Message):
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("Команда работает только в рабочей группе или топике.")
        return
    thread_id = getattr(message, "message_thread_id", None)
    await message.answer(render_group_finance(message.chat.id, thread_id))

@router.callback_query(F.data == "admin:group_finance_panel")
async def admin_group_finance_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await safe_edit_or_send(callback, "<b>🏦 Выберите группу / топик для казны:</b>", reply_markup=group_finance_list_kb())
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:groupfin:"))
async def admin_group_finance_open(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id = callback.data.split(":")
    chat_id = int(chat_id)
    thread_id = None if int(thread_id) == 0 else int(thread_id)
    await safe_edit_or_send(callback, render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))
    await safe_callback_answer(callback)

@router.callback_query(F.data.startswith("admin:groupfin_add:"))
@router.callback_query(F.data.startswith("admin:groupfin_sub:"))
async def admin_group_finance_change_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split(":")
    action = "add" if parts[1].endswith("add") else "sub"
    chat_id = int(parts[2])
    thread_id = None if int(parts[3]) == 0 else int(parts[3])
    await state.set_state(AdminStates.waiting_group_finance_amount)
    await state.update_data(group_fin_action=action, group_fin_chat_id=chat_id, group_fin_thread_id=thread_id)
    title = escape(workspace_display_title(chat_id, thread_id))
    label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    await callback.message.answer(f"Введите сумму для действия <b>{'пополнить' if action == 'add' else 'списать'}</b> в группе <b>{title}</b>\n{label}:")
    await safe_callback_answer(callback)

@router.message(AdminStates.waiting_group_finance_amount)
async def admin_group_finance_amount(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or '').replace(',', '.').replace('$', '').strip())
    except Exception:
        await message.answer("Введите сумму числом.")
        return
    if value <= 0:
        await message.answer("Сумма должна быть больше 0.")
        return
    data = await state.get_data()
    chat_id = int(data['group_fin_chat_id'])
    thread_id = data.get('group_fin_thread_id')
    if data.get('group_fin_action') == 'add':
        db.add_group_balance(chat_id, thread_id, value)
    else:
        if value > db.get_group_balance(chat_id, thread_id):
            await message.answer("Недостаточно средств в казне группы.")
            return
        db.subtract_group_balance(chat_id, thread_id, value)
    await state.clear()
    await message.answer(render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))

@router.callback_query(F.data.startswith("admin:groupprice:"))
async def admin_group_price_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    _, _, chat_id, thread_id, mode, operator_key = callback.data.split(":")
    thread_id = None if int(thread_id) == 0 else int(thread_id)
    await state.set_state(AdminStates.waiting_group_price_value)
    await state.update_data(group_price_chat_id=int(chat_id), group_price_thread_id=thread_id, price_mode=mode, operator_key=operator_key)
    label = f"<code>{chat_id}</code>" + (f" / topic <code>{thread_id}</code>" if thread_id else "")
    await callback.message.answer(f"Введите цену для группы {label}: {op_text(operator_key)} • <b>{mode_label(mode)}</b>")
    await safe_callback_answer(callback)

@router.message(AdminStates.waiting_group_price_value)
async def admin_group_price_value(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        value = float((message.text or '').replace(',', '.').replace('$', '').strip())
    except Exception:
        await message.answer("Введите цену числом.")
        return
    if value <= 0:
        await message.answer("Цена должна быть больше 0.")
        return
    data = await state.get_data()
    chat_id = int(data['group_price_chat_id'])
    thread_id = data.get('group_price_thread_id')
    db.set_group_price(chat_id, thread_id, data['operator_key'], data['price_mode'], value)
    await state.clear()
    await message.answer(render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))

@router.message()
async def track_any_message(message: Message):
    try:
        if message.from_user:
            touch_user(message.from_user.id, message.from_user.username or '', message.from_user.full_name)
    except Exception:
        logging.exception("track_any_message failed")



def cleanup_database_size(max_mb: int = 18):
    """Сжимает bot.db, чтобы база не раздувалась от старых QR/blob.

    Номера, операторы, история, статистика и file_id остаются.
    Удаляются только тяжёлые qr_blob у старых закрытых заявок, когда база выше лимита.
    Активные заявки и последние QR сохраняются.
    """
    try:
        db_path = Path(DB_PATH)
        if not db_path.exists():
            return
        before = db_path.stat().st_size / (1024 * 1024)
        if before <= max_mb:
            return
        logging.warning("DB cleanup started: %.2f MB > %s MB", before, max_mb)

        # Убираем blob только у старых закрытых заявок. qr_file_id/номер/оператор остаются.
        try:
            db.conn.execute("""
                UPDATE queue_items
                   SET qr_blob=NULL, qr_mime=NULL, qr_filename=NULL
                 WHERE qr_blob IS NOT NULL
                   AND status IN ('completed','paid','failed','slipped','cancelled')
                   AND id NOT IN (
                       SELECT id FROM queue_items
                        WHERE qr_blob IS NOT NULL
                        ORDER BY id DESC
                        LIMIT 300
                   )
            """)
            db.conn.commit()
        except Exception:
            logging.exception("DB cleanup closed qr_blob cleanup failed")

        # Если всё ещё большая — оставляем активные + последние 150 blob.
        try:
            mid = db_path.stat().st_size / (1024 * 1024)
            if mid > max_mb:
                db.conn.execute("""
                    UPDATE queue_items
                       SET qr_blob=NULL, qr_mime=NULL, qr_filename=NULL
                     WHERE qr_blob IS NOT NULL
                       AND status NOT IN ('queued','taken','in_progress')
                       AND id NOT IN (
                           SELECT id FROM queue_items
                            WHERE qr_blob IS NOT NULL
                            ORDER BY id DESC
                            LIMIT 150
                       )
                """)
                db.conn.commit()
        except Exception:
            logging.exception("DB cleanup stronger qr_blob cleanup failed")

        # Чистим возможные служебные логи, если такие таблицы есть.
        for table in ("logs", "event_logs", "admin_logs"):
            try:
                db.conn.execute(f"DELETE FROM {table} WHERE id NOT IN (SELECT id FROM {table} ORDER BY id DESC LIMIT 5000)")
                db.conn.commit()
            except Exception:
                pass

        try:
            db.conn.execute("VACUUM")
            db.conn.commit()
        except Exception:
            logging.exception("DB cleanup VACUUM failed")

        after = db_path.stat().st_size / (1024 * 1024)
        logging.warning("DB cleanup finished: %.2f MB -> %.2f MB", before, after)
    except Exception:
        logging.exception("cleanup_database_size failed")







@router.callback_query(F.data == "admin:cleanup_queue")
async def admin_cleanup_queue_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback)
    all_count, hold_count = active_queue_counts_for_admin()
    await callback.message.answer(
        "🧹 <b>Очистка очередей</b>\n\n"
        f"Активных заявок всего: <b>{all_count}</b>\n"
        f"Активных холдов: <b>{hold_count}</b>\n\n"
        "Выберите действие:",
        reply_markup=admin_cleanup_queue_keyboard()
    )

@router.callback_query(F.data == "admin:close_all_holds_confirm")
async def admin_close_all_holds_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, закрыть все холды", callback_data="admin:close_all_holds_no_pay")
    kb.button(text="↩️ Отмена", callback_data="admin:panel")
    kb.adjust(1)
    await callback.message.answer(
        "⚠️ <b>Закрыть все холды без оплаты?</b>\n\n"
        "Все активные заявки режима <b>Холд</b> будут закрыты.\n"
        "Баланс пользователям <b>не начисляется</b>.\n"
        "В очереди/на холде у людей они больше отображаться не будут.",
        reply_markup=kb.as_markup()
    )


@router.callback_query(F.data == "admin:close_all_holds_no_pay")
async def admin_close_all_holds_no_pay_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback, "Закрываю холды...")
    count = admin_close_all_holds_no_pay(callback.from_user.id)
    await callback.message.answer(
        f"✅ Закрыто холдов без оплаты: <b>{count}</b>\n\n"
        "Балансы не изменялись. Активные статусы у пользователей очищены."
    )


@router.callback_query(F.data == "admin:remove_all_queue_confirm")
async def admin_remove_all_queue_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback)
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Да, убрать всё из очереди", callback_data="admin:remove_all_queue")
    kb.button(text="↩️ Отмена", callback_data="admin:panel")
    kb.adjust(1)
    await callback.message.answer(
        "⚠️ <b>Убрать всё из очереди?</b>\n\n"
        "Все активные заявки будут закрыты без оплаты.\n"
        "Балансы пользователям <b>не начисляются</b>.\n"
        "У людей больше не будет висеть статус <b>в очереди / в обработке / на холде</b>.",
        reply_markup=kb.as_markup()
    )


@router.callback_query(F.data == "admin:remove_all_queue")
async def admin_remove_all_queue_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback, "Очищаю очередь...")
    count = admin_remove_all_from_queue(callback.from_user.id)
    await callback.message.answer(
        f"✅ Убрано активных заявок из очереди: <b>{count}</b>\n\n"
        "Балансы не изменялись. Активные статусы у пользователей очищены."
    )

@router.callback_query(F.data == "admin:dbcompact")
async def admin_dbcompact_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await safe_callback_answer(callback, "Нет доступа")
        return
    await safe_callback_answer(callback, "Готовлю облегчённую копию БД...")
    path = make_compact_db_copy(18)
    if not path or not path.exists():
        await callback.message.answer("❌ Не удалось сделать облегчённую копию БД.")
        return
    size_mb = path.stat().st_size / (1024 * 1024)
    await callback.message.answer_document(
        FSInputFile(path),
        caption=(
            "✅ Облегчённая копия БД\n\n"
            f"Размер ZIP: <b>{size_mb:.2f} MB</b>\n"
            "Внутри файл: <code>bot.db</code>\n\n"
            "Эта выгрузка не откатывает баланс и очередь."
        )
    )

@router.message(Command("dbcompact"))
async def dbcompact_cmd(message: Message):
    await send_compact_db_export(message)


@router.message(Command("db_export_compact"))
async def db_export_compact_cmd(message: Message):
    await send_compact_db_export(message)






@router.message(Command("closeholds"))
async def closeholds_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return
    count = admin_close_all_holds_no_pay(message.from_user.id)
    await message.answer(
        f"✅ Закрыто холдов без оплаты: <b>{count}</b>\n\n"
        "Балансы не изменялись. У пользователей эти заявки больше не активны."
    )


@router.message(Command("clearqueue"))
async def clearqueue_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return
    count = admin_remove_all_from_queue(message.from_user.id)
    await message.answer(
        f"✅ Убрано активных заявок из очереди: <b>{count}</b>\n\n"
        "Балансы не изменялись. У пользователей эти заявки больше не активны."
    )


@router.message(Command("adminclear"))
async def adminclear_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return
    all_count, hold_count = active_queue_counts_for_admin()
    await message.answer(
        "🧹 <b>Очистка очередей</b>\n\n"
        f"Активных заявок всего: <b>{all_count}</b>\n"
        f"Активных холдов: <b>{hold_count}</b>\n\n"
        "Можно нажать кнопки ниже или использовать команды:\n"
        "<code>/closeholds</code> — закрыть все холды без оплаты\n"
        "<code>/clearqueue</code> — убрать всё из очереди",
        reply_markup=admin_cleanup_queue_keyboard()
    )

async def main():
    global LIVE_DP, PRIMARY_BOT
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set it in Railway Variables.")

    db.recover_after_restart()
    primary_bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    PRIMARY_BOT = primary_bot
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    LIVE_DP = dp

    asyncio.create_task(hold_watcher(primary_bot))
    asyncio.create_task(backup_watcher(primary_bot))

    try:
        me = await primary_bot.get_me()
        db.set_setting('bot_username_cached', me.username or BOT_USERNAME_FALLBACK)
        try:
            await primary_bot.set_chat_menu_button(menu_button=MenuButtonCommands())
            logging.info("Bot menu button reset to default commands")
        except Exception:
            logging.exception("reset bot menu button failed")
        logging.info("Primary bot started as @%s", me.username or BOT_USERNAME_FALLBACK)
        logging.info("Anti-crash recovery complete; holds and queue state restored")
    except Exception:
        logging.exception("Primary bot get_me failed")

    for mirror in db.all_active_mirrors():
        token = (mirror["token"] or "").strip()
        if not token or token == BOT_TOKEN:
            continue
        await start_live_mirror(token)

    cleanup_database_size(18)
    await dp.start_polling(primary_bot)


if __name__ == "__main__":
    asyncio.run(main())
