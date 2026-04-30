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
from aiogram.types import BufferedInputFile, CallbackQuery, FSInputFile, Message, ForceReply, InlineKeyboardButton, MenuButtonWebApp, WebAppInfo, InputMediaPhoto
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

# =========================================================
# CONFIG - ALL IN ONE FILE
# =========================================================
def load_local_env(env_path: str = ".env"):
  try:
    env_file = Path(env_path)
    if not env_file.exists():
      return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
      line = raw_line.strip()
      if not line or line.startswith("#") or "=" not in line:
        continue
      key, value = line.split("=", 1)
      key = key.strip()
      value = value.strip().strip('"').strip("'")
      if key and key not in os.environ:
        os.environ[key] = value
  except Exception:
    logging.exception("Failed to load .env file")

load_local_env()
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
if not BOT_TOKEN:
  raise RuntimeError("BOT_TOKEN is missing. Put it into the .env file or environment variables.")
DB_PATH = (os.getenv("DB_PATH") or "/data/bot.db").strip()
BOT_USERNAME_FALLBACK = "DiamondVaultV_bot"

# Roles
CHIEF_ADMIN_ID = 626387429
BOOTSTRAP_ADMINS = [626387429]
BOOTSTRAP_OPERATORS = []

WITHDRAW_CHANNEL_ID = -1003827772392
LOG_CHANNEL_ID = -1003736283466
MIN_WITHDRAW = 10.0
DEFAULT_HOLD_MINUTES = 15
DEFAULT_TREASURY_BALANCE = 0.0

# Crypto Bot / Crypto Pay API
CRYPTO_PAY_TOKEN = "" # disabled
CRYPTO_PAY_BASE_URL = "https://pay.crypt.bot/api"
CRYPTO_PAY_ASSET = "USDT"
CRYPTO_PAY_PIN_CHECK_TO_USER = False # True -> check pinned to telegram user

BASE_OPERATORS = {
  "mts": {"title": "МТС", "price": 4.00, "command": "/mts"},
  "mts_premium": {"title": "МТС Салон", "price": 4.00, "command": "/mtspremium"},
  "bil": {"title": "Билайн", "price": 4.50, "command": "/bil"},
  "mega": {"title": "Мегафон", "price": 5.00, "command": "/mega"},
  "t2": {"title": "Tele2", "price": 4.20, "command": "/t2"},
  "vtb": {"title": "ВТБ", "price": 4.80, "command": "/vtb"},
  "gaz": {"title": "Газпром", "price": 4.90, "command": "/gaz"},
}

OPERATORS = {k: dict(v) for k, v in BASE_OPERATORS.items()}
# =========================================================

START_BANNER = "start_banner.jpg"
PROFILE_BANNER = "profile_banner.jpg"
MY_NUMBERS_BANNER = "my_numbers_banner.jpg"
WITHDRAW_BANNER = "withdraw_banner.jpg"
MSK_OFFSET = timedelta(hours=3)
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", "8080"))
WEBAPP_BASE_URL = (os.getenv("WEBAPP_BASE_URL") or "https://vault-production-67a7.up.railway.app").rstrip("/")
MINI_PROFILE_BANNER = "mini_profile_banner.jpg"
MINI_MANUALS_BANNER = "mini_manuals_banner.jpg"

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
PRIORITY_USER_ID = 713807432
PRIORITY_USER_USERNAME = "oveiro"



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
  waiting_miniapp_text = State()
  waiting_miniapp_submit_bot = State()
  waiting_page_jump = State()


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
    self._ensure_db_dir()
    self.conn = sqlite3.connect(path)
    self.conn.row_factory = sqlite3.Row
    self.create_tables()
    self.seed_defaults()

  def _ensure_db_dir(self):
    try:
      parent = Path(self.path).expanduser().resolve().parent
    except Exception:
      parent = Path(self.path).parent
    if str(parent) not in ('', '.'): 
      parent.mkdir(parents=True, exist_ok=True)

  def reconnect(self):
    try:
      self.conn.close()
    except Exception:
      pass
    self._ensure_db_dir()
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
      shutil.copyfile(current_path, backup_path)
    try:
      self.conn.close()
    except Exception:
      pass
    shutil.move(str(temp_uploaded), self.path)
    self.conn = sqlite3.connect(self.path)
    self.conn.row_factory = sqlite3.Row
    self.create_tables()
    self.seed_defaults()
    try:
      ensure_extra_schema()
    except Exception:
      pass
    try:
      reload_operators_from_db()
    except Exception:
      logging.exception("reload_operators_from_db failed after db replace")
    return backup_path

  def import_users_from_uploaded_db(self, uploaded_path: str):
    source_path = Path(uploaded_path)
    conn = sqlite3.connect(str(source_path))
    conn.row_factory = sqlite3.Row
    try:
      table_row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND lower(name)='users' LIMIT 1").fetchone()
      if not table_row:
        raise ValueError("В загруженной базе нет таблицы users")
      table_name = table_row[0]
      cols = {r['name'] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
      if 'user_id' not in cols:
        raise ValueError("В таблице users нет столбца user_id")
      select_cols = ['user_id']
      for col in ('username', 'full_name', 'balance', 'created_at'):
        if col in cols:
          select_cols.append(col)
      rows = conn.execute(f"SELECT {', '.join(select_cols)} FROM {table_name}").fetchall()
    finally:
      conn.close()

    imported = 0
    updated = 0
    skipped = 0
    cur = self.conn.cursor()
    for row in rows:
      row = dict(row)
      user_id = row.get('user_id')
      if user_id is None:
        skipped += 1
        continue
      existing = cur.execute("SELECT user_id, username, full_name, balance, created_at FROM users WHERE user_id=?", (user_id,)).fetchone()
      username = row.get('username') if row.get('username') is not None else (existing['username'] if existing else None)
      full_name = row.get('full_name') if row.get('full_name') is not None else (existing['full_name'] if existing else '')
      balance = row.get('balance') if row.get('balance') is not None else (existing['balance'] if existing else 0)
      created_at = row.get('created_at') if row.get('created_at') else (existing['created_at'] if existing else now_str())
      if existing:
        cur.execute(
          "UPDATE users SET username=?, full_name=?, balance=?, created_at=? WHERE user_id=?",
          (username, full_name, balance, created_at, user_id),
        )
        updated += 1
      else:
        cur.execute(
          "INSERT INTO users (user_id, username, full_name, balance, created_at) VALUES (?, ?, ?, ?, ?)",
          (user_id, username, full_name, balance, created_at),
        )
        imported += 1
    self.conn.commit()
    return {'total': len(rows), 'imported': imported, 'updated': updated, 'skipped': skipped}

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
        timer_last_render TEXT
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
      "start_title": "ESIM Diamond Vault",
      "start_subtitle": "Ваш eSIM под надежной защитой Diamond Vault 💎",
      "start_description": "🚀 <b>Быстрый приём заявок</b> • 💎 <b>Надёжный сервис</b> • 🛡 <b>Контроль статусов</b>",
      "announcement_text": "",
      "backup_channel_id": "0",
      "backup_enabled": "0",
    }
    for key, value in defaults.items():
      self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
    for key, data in OPERATORS.items():
      self.conn.execute(
        "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
        (f"price_{key}", str(data["price"])),
      )
      self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_hold_{key}", "1"))
      self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_no_hold_{key}", "1"))
      self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_hold_{key}", "1"))
      self.conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (f"allow_no_hold_{key}", "1"))
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


async def message_actor_can_take_esim(message: Message) -> tuple[bool, str]:
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
    status = getattr(member, "status", "unknown")
    if status in {"creator", "administrator"}:
      return True, f"chat_admin:{status}"
    return False, f"chat_member:{status}"
  except Exception:
    logging.exception("message_actor_can_take_esim failed chat_id=%s user_id=%s", message.chat.id, user.id)
    return False, f"internal_role:{role or 'none'}"


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
    'Для доступа к функционалу нужна обязательная подписка на указанную группу.\n\n'
    'После вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.'
  )
  await send_banner_message(entity, db.get_setting('start_banner_path', START_BANNER), text, required_join_kb().as_markup())
  return False

def main_menu():
  kb = InlineKeyboardBuilder()
  kb.button(text="📲 Сдать eSIM", callback_data="menu:submit")
  kb.button(text="📦 Мои номера", callback_data="menu:my")
  kb.button(text="👤 Личный кабинет", callback_data="menu:profile")
  kb.button(text="👥 Реф. система", callback_data="menu:ref")
  kb.button(text="🏦 Вывод средств", callback_data="menu:withdraw")
  kb.button(text="📚 Мануалы", callback_data="menu:manuals")
  kb.button(text="🔗 Зеркало", callback_data="menu:mirror")
  kb.adjust(2, 2, 2, 1)
  url = miniapp_url('/')
  if url:
    kb.row(InlineKeyboardButton(text="DVE", web_app=WebAppInfo(url=url)))
  else:
    kb.row(InlineKeyboardButton(text="DVE", callback_data="miniapp:help"))
  return kb.as_markup()


def profile_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="📦 Мои номера", callback_data="menu:my")
  kb.button(text="👥 Реф. система", callback_data="menu:ref")
  kb.button(text="💳 Изменить реквизиты", callback_data="menu:payout_link")
  kb.button(text="🏦 Вывод средств", callback_data="menu:withdraw")
  kb.button(text="🏠 На главную", callback_data="menu:home")
  kb.adjust(1)
  return kb.as_markup()

def my_numbers_kb(items):
  kb = InlineKeyboardBuilder()
  for item in items[:10]:
    if item['status'] == 'queued':
      kb.button(text=f"🗑 Убрать #{item['id']}", callback_data=f"myremove:{item['id']}")
  kb.button(text="🔄 Обновить список", callback_data="menu:my")
  kb.button(text="🏠 На главную", callback_data="menu:home")
  kb.adjust(1)
  return kb.as_markup()



def quick_submit_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="➕ Добавить ещё", callback_data="menu:submit")
  kb.button(text="🏠 На главную", callback_data="menu:home")
  kb.adjust(1)
  return kb.as_markup()

def mirror_menu_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="➕ Создать зеркало", callback_data="mirror:create")
  kb.button(text="📋 Мои зеркала", callback_data="mirror:list")
  kb.button(text="🏠 На главную", callback_data="menu:home")
  kb.adjust(1)
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
  kb.button(text="⚡ Безхолд", callback_data="esim_mode:no_hold")
  kb.button(text="🏠 Закрыть", callback_data="noop")
  kb.adjust(2, 1)
  return kb.as_markup()


def mode_inline_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="⏳ Холд", callback_data="mode:hold")
  kb.button(text="⚡ Безхолд", callback_data="mode:no_hold")
  kb.button(text="↩️ Назад", callback_data="menu:submit")
  kb.adjust(2, 1)
  return kb.as_markup()


def mode_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="⏳ Холд", callback_data="mode:hold")
  kb.button(text="⚡ Безхолд", callback_data="mode:no_hold")
  kb.button(text="↩️ Назад", callback_data="mode:back")
  kb.adjust(2, 1)
  return kb.as_markup()

def submit_result_kb(operator_key: str, mode: str):
  kb = InlineKeyboardBuilder()
  kb.button(text="📲 Добавить ещё", callback_data=f"submit_more:{operator_key}:{mode}")
  kb.button(text="✅ Готово", callback_data="menu:home")
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
  kb.button(text="📊 Общий отчет", callback_data="admin:summary")
  kb.button(text="📈 Отчёты групп", callback_data="admin:group_stats_panel")
  kb.button(text="🏦 Выплаты", callback_data="admin:withdraws")
  kb.button(text="🏦 Казна групп", callback_data="admin:group_finance_panel")
  kb.button(text="⏳ Холд", callback_data="admin:hold")
  kb.button(text="💎 Прайсы", callback_data="admin:prices")
  kb.button(text="➕ Добавить оператора", callback_data="admin:add_operator")
  kb.button(text="💎 Эмодзи операторов", callback_data="admin:set_operator_emoji")
  kb.button(text="➖ Удалить оператора", callback_data="admin:remove_operator")
  kb.button(text="🛡 Роли", callback_data="admin:roles")
  kb.button(text="🛰 Рабочие зоны", callback_data="admin:workspaces")
  kb.button(text="📦 Очередь", callback_data="admin:queues")
  kb.button(text="🖼 QR и номера", callback_data="admin:qr_numbers")
  kb.button(text="👤 Пользователь", callback_data="admin:user_tools")
  kb.button(text="⚙️ Настройки", callback_data="admin:settings")
  kb.adjust(2,2,2,2,2,2,2,1)
  return kb.as_markup()


def operator_emoji_pick_kb():
  kb = InlineKeyboardBuilder()
  for key in OPERATORS:
    kb.button(text=op_text(key), callback_data=f"admin:pick_operator_emoji:{key}")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
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
    kb.adjust(*([2] * len(seen)), 1)
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
  kb.adjust(1)
  return kb.as_markup()

def group_finance_manage_kb(chat_id: int, thread_id: int | None):
  kb = InlineKeyboardBuilder()
  kb.button(text="➕ Пополнить", callback_data=f"admin:groupfin_add:{chat_id}:{thread_id or 0}")
  kb.button(text="➖ Списать", callback_data=f"admin:groupfin_sub:{chat_id}:{thread_id or 0}")
  for mode in ('hold', 'no_hold'):
    for key in OPERATORS:
      icon = '⏳' if mode == 'hold' else '⚡'
      kb.button(text=f"{icon} {op_text(key)}", callback_data=f"admin:groupprice:{chat_id}:{thread_id or 0}:{mode}:{key}")
  kb.button(text="↩️ К списку групп", callback_data="admin:group_finance_panel")
  kb.adjust(2,2,2,2,2,2,1)
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
      SUM(COALESCE(charge_amount, price)) AS turnover_total
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
      f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)}) • "
      f"🏦 <b>{usd(row['turnover_total'] or 0)}</b>"
    )
  if not op_lines:
    op_lines = ["• Данных пока нет"]

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
    f"💰 Выплачено пользователям: <b>{usd(totals['paid_total'] or 0)}</b>\n"
    f"🏦 Списано с казны: <b>{usd(totals['spent_total'] or 0)}</b>\n"
    f"📈 Маржа группы: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
    "<b>📱 По операторам</b>\n" + "\n".join(op_lines) + "\n\n"
    "<b>👥 Разбор по взявшим</b>\n" + "\n".join(taker_lines)
  )

def single_group_stats_kb(chat_id: int, thread_id: int | None):
  kb = InlineKeyboardBuilder()
  kb.button(text="🗑 Убрать группу", callback_data=f"admin:group_remove:{chat_id}:{thread_id or 0}")
  kb.button(text="↩️ К списку групп", callback_data="admin:group_stats_panel")
  kb.adjust(1)
  return kb.as_markup()

def user_price_operator_kb(target_user_id: int):
  kb = InlineKeyboardBuilder()
  for key in OPERATORS:
    kb.button(text=op_text(key), callback_data=f"admin:user_price_op:{target_user_id}:{key}")
  kb.button(text="❌ Отмена", callback_data="admin:user_tools")
  kb.adjust(1)
  return kb.as_markup()

def user_price_mode_kb(target_user_id: int, operator_key: str):
  kb = InlineKeyboardBuilder()
  kb.button(text="⏳ Холд", callback_data=f"admin:user_price_mode:{target_user_id}:{operator_key}:hold")
  kb.button(text="⚡ Безхолд", callback_data=f"admin:user_price_mode:{target_user_id}:{operator_key}:no_hold")
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
  kb.adjust(1)
  return kb.as_markup()


def _start_of_today_str() -> str:
  return datetime.now().strftime("%Y-%m-%d 00:00:00")


def admin_operator_filter_buttons(kb: InlineKeyboardBuilder, prefix: str, current_operator: str = "all", page: int = 0, status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  buttons = [("Все", "all")] + [(OPERATORS[key]['title'], key) for key in OPERATORS.keys()]
  for title, key in buttons:
    marker = "• " if key == current_operator else ""
    kb.button(text=(marker + title)[:32], callback_data=f"{prefix}:{key}:{status_filter}:{day_filter}:{page}:{mode_filter}")


def admin_scope_filter_buttons(kb: InlineKeyboardBuilder, prefix: str, current_operator: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  all_mark = "• " if status_filter == "all" else ""
  queue_mark = "• " if status_filter == "queued" else ""
  whole_mark = "• " if day_filter == "all" else ""
  today_mark = "• " if day_filter == "today" else ""
  kb.button(text=(all_mark + "Все статусы")[:32], callback_data=f"{prefix}:{current_operator}:all:{day_filter}:0:{mode_filter}")
  kb.button(text=(queue_mark + "Только в очереди")[:32], callback_data=f"{prefix}:{current_operator}:queued:{day_filter}:0:{mode_filter}")
  kb.button(text=(whole_mark + "За всё время")[:32], callback_data=f"{prefix}:{current_operator}:{status_filter}:all:0:{mode_filter}")
  kb.button(text=(today_mark + "За сегодня")[:32], callback_data=f"{prefix}:{current_operator}:{status_filter}:today:0:{mode_filter}")


def admin_mode_filter_buttons(kb: InlineKeyboardBuilder, prefix: str, current_operator: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  all_mark = "• " if mode_filter == "all" else ""
  hold_mark = "• " if mode_filter == "hold" else ""
  no_hold_mark = "• " if mode_filter == "no_hold" else ""
  kb.button(text=(all_mark + "Все режимы")[:32], callback_data=f"{prefix}:{current_operator}:{status_filter}:{day_filter}:0:all")
  kb.button(text=(hold_mark + "Только холд")[:32], callback_data=f"{prefix}:{current_operator}:{status_filter}:{day_filter}:0:hold")
  kb.button(text=(no_hold_mark + "Только без холда")[:32], callback_data=f"{prefix}:{current_operator}:{status_filter}:{day_filter}:0:no_hold")


def queue_page_items(page: int = 0, per_page: int = 10, operator_key: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  page = max(0, int(page))
  params = []
  where = "WHERE status='queued'"
  if operator_key != "all":
    where += " AND operator_key=?"
    params.append(operator_key)
  if mode_filter in {"hold", "no_hold"}:
    where += " AND mode=?"
    params.append(mode_filter)
  if day_filter == "today":
    where += " AND created_at>=?"
    params.append(_start_of_today_str())
  total = db.conn.execute(f"SELECT COUNT(*) AS c FROM queue_items {where}", tuple(params)).fetchone()['c'] or 0
  offset = page * per_page
  rows = db.conn.execute(
    f"SELECT * FROM queue_items {where} ORDER BY id DESC LIMIT ? OFFSET ?",
    tuple(params + [per_page, offset])
  ).fetchall()
  return rows, int(total)


def queue_manage_kb(page: int = 0, operator_key: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  kb = InlineKeyboardBuilder()
  items, total = queue_page_items(page, 10, operator_key, day_filter, mode_filter)
  admin_operator_filter_buttons(kb, "admin:queue_filter", operator_key, page, "queued", day_filter, mode_filter)
  admin_scope_filter_buttons(kb, "admin:queue_filter", operator_key, "queued", day_filter, mode_filter)
  admin_mode_filter_buttons(kb, "admin:queue_filter", operator_key, "queued", day_filter, mode_filter)
  for item in items:
    short_op = op_text(item['operator_key']).replace('🟡 ', '').replace('⚫ ', '').replace('🟢 ', '').replace('🔴 ', '').replace('🔵 ', '').replace('⚪ ', '')
    label = f"#{item['id']} • {short_op} • {pretty_phone(item['normalized_phone'])}"
    kb.button(text=f"👁 {label}"[:64], callback_data=f"admin:queue_view:{item['id']}:{page}:{operator_key}:{day_filter}:{mode_filter}")
    kb.button(text=f"🗑 {label}"[:64], callback_data=f"admin:queue_remove:{item['id']}:{page}:{operator_key}:{day_filter}:{mode_filter}")
  total_pages = max(1, (int(total) + 9) // 10)
  safe_page = min(max(0, int(page)), total_pages - 1)
  prev_page = safe_page - 1 if safe_page > 0 else total_pages - 1
  next_page = safe_page + 1 if safe_page < total_pages - 1 else 0
  kb.button(text="⬅️", callback_data=f"admin:queues:{prev_page}:{operator_key}:{day_filter}:{mode_filter}")
  kb.button(text=f"Стр. {safe_page + 1}/{total_pages}", callback_data="admin:queues:noop")
  kb.button(text="➡️", callback_data=f"admin:queues:{next_page}:{operator_key}:{day_filter}:{mode_filter}")
  kb.button(text="🔢 Страница", callback_data=f"admin:queue_jump:{safe_page}:{operator_key}:{day_filter}:{mode_filter}")
  kb.button(text="🔄 Обновить", callback_data=f"admin:queues:{safe_page}:{operator_key}:{day_filter}:{mode_filter}")
  kb.button(text="🖼 QR и номера", callback_data=f"admin:qr_numbers:0:{operator_key}:queued:{day_filter}:{mode_filter}")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  if items:
    kb.adjust(3, 2, 3, *([2] * len(items)), 3, 3, 1)
  else:
    kb.adjust(3, 2, 3, 3, 3, 1)
  return kb.as_markup()


def roles_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="👑 Назначить главного", callback_data="admin:role:chief_admin")
  kb.button(text="🛡 Назначить админа", callback_data="admin:role:admin")
  kb.button(text="🎧 Назначить оператора", callback_data="admin:role:operator")
  kb.button(text="🗑 Снять роль", callback_data="admin:role:remove")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
  return kb.as_markup()


def workspaces_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="➕ Добавить рабочую группу", callback_data="admin:ws_help_group")
  kb.button(text="➕ Добавить топик", callback_data="admin:ws_help_topic")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
  return kb.as_markup()


def design_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="✍️ Изменить старт", callback_data="admin:set_start_text")
  kb.button(text="📣 Изменить объявление", callback_data="admin:set_ad_text")
  kb.button(text="🧩 Шаблоны", callback_data="admin:templates")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
  return kb.as_markup()


def broadcast_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="📨 Написать рассылку", callback_data="admin:broadcast_write")
  kb.button(text="👀 Превью объявления", callback_data="admin:broadcast_preview")
  kb.button(text="🚀 Разослать объявление", callback_data="admin:broadcast_send_ad")
  kb.button(text="📥 Скачать username", callback_data="admin:usernames")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
  return kb.as_markup()


def escape(value: Optional[str]) -> str:
  return html.escape(str(value or "-"))


def queue_caption(item: QueueItem) -> str:
  display_price = getattr(item, 'charge_amount', None)
  if display_price in (None, 0, 0.0):
    charge_chat_id = getattr(item, 'charge_chat_id', None)
    if charge_chat_id:
      display_price = group_price_for_take(charge_chat_id, getattr(item, 'charge_thread_id', None), item.operator_key, item.mode)
  text = (
    f"📱 {op_html(item.operator_key)}\n\n"
    f"🧾 Заявка: <b>{item.id}</b>\n"
    f"👤 От: <b>{escape(item.full_name)}</b>\n"
    f"🆔 ID: <code>{item.user_id}</code>\n"
    f"📞 Номер: <code>{escape(pretty_phone(item.normalized_phone))}</code>\n"
    f"🔄 Режим: <b>{'Холд' if item.mode == 'hold' else 'БезХолд'}</b>"
  )
  if display_price not in (None, 0, 0.0):
    text += f"\n🏷 Прайс группы: <b>{usd(float(display_price))}</b>"
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
    "<b>🤝 Партнёрская программа</b>\n\n"
    + quote_block([
      "💸 Вы получаете <b>5%</b> с заработка каждого приглашённого пользователя.",
      f"👥 <b>Ваших рефералов:</b> {ref_count}",
      f"💰 <b>Заработано по рефке:</b> <b>{usd(ref_earned)}</b>",
      f"🔗 <b>Ваша ссылка:</b> <code>{escape(link)}</code>",
    ])
    + "\n\nПоделитесь ссылкой с другом. После старта в боте он автоматически закрепится за вами.\n\n"
    + "Начисление приходит после того, как приглашённый пользователь получает оплату за успешно обработанный номер."
  )

def referral_kb(user_id: int):
  kb = InlineKeyboardBuilder()
  kb.button(text="🔄 Обновить", callback_data="menu:ref")
  kb.button(text="👤 Личный кабинет", callback_data="menu:profile")
  kb.button(text="🏠 На главную", callback_data="menu:home")
  kb.adjust(1)
  return kb.as_markup()



def webapp_base_url() -> str:
  if WEBAPP_BASE_URL:
    return WEBAPP_BASE_URL
  cached = (db.get_setting("webapp_base_url", "") or "").strip().rstrip("/")
  return cached


def miniapp_url(path: str = "/") -> str:
  base = webapp_base_url()
  if not base:
    return ""
  return f"{base}{path}"


def miniapp_home_kb():
  kb = InlineKeyboardBuilder()
  url = miniapp_url('/')
  if url:
    kb.button(text="DVE", web_app=WebAppInfo(url=url))
  else:
    kb.button(text="ℹ️ Mini App не настроен", callback_data="miniapp:help")
  kb.button(text="🏠 На главную", callback_data="menu:home")
  kb.adjust(1)
  return kb.as_markup()


def miniapp_help_text() -> str:
  return (
    "<b>✨ Mini App</b>\n\n"
    "Укажи домен Railway в переменной <code>WEBAPP_BASE_URL</code>, чтобы встроенное меню открывалось прямо внутри Telegram.\n\n"
    "<b>Пример:</b> <code>https://your-project.up.railway.app</code>"
  )



def miniapp_home_html(bot_username: str) -> str:
  bot_link = f"https://t.me/{bot_username}" if bot_username else "https://t.me/"
  submit_link = '/submit'
  body = """
  <div class="box pad">
    <div style="display:grid;grid-template-columns:minmax(0,1fr) 92px;gap:12px;align-items:start;">
      <div><small class="eyebrow">Diamond Vault Esim</small><h1 class="h1">DVE APP</h1><p class="lead">Главный центр: сдача eSIM, мануалы, профиль и быстрый доступ к разделам.</p></div>
      <div style="width:92px;height:92px;border-radius:22px;overflow:hidden;border:1px solid rgba(236,194,107,.24);background:#120d0b;"><img id="tgAvatar" alt="avatar" style="width:100%;height:100%;object-fit:cover;display:none"><div id="avatarFallback" style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:34px;color:#d9be8c;">👤</div></div>
    </div>
    <div class="box" style="overflow:hidden;margin-top:14px;padding:10px;"><img src="/mini_profile_banner.jpg" alt="profile" style="width:100%;height:auto;display:block;border-radius:18px;"></div>
  </div>
  <div class="box pad" style="margin-top:14px;"><small class="eyebrow">Быстрый доступ</small><div class="grid2"><a class="btn primary cardbtn" href="__SUBMIT__"><span style="font-size:26px;">📲</span><span style="font-size:21px;">Сдать eSIM</span><span class="muted cardhint">Создать новую заявку и перейти к отправке QR.</span></a><a class="btn gold cardbtn" href="/manuals"><span style="font-size:26px;">📚</span><span style="font-size:21px;">Мануалы</span><span class="muted cardhint">Открыть библиотеку материалов и разделы по операторам.</span></a><a class="btn blue cardbtn" href="/profile"><span style="font-size:26px;">👤</span><span style="font-size:21px;">Профиль</span><span class="muted cardhint">Тег, ID, баланс и статистика аккаунта.</span></a><a class="btn secondary cardbtn" href="/numbers"><span style="font-size:26px;">📦</span><span style="font-size:21px;">Мои номера</span><span class="muted cardhint">Список заявок, статусы и место в очереди.</span></a></div></div>
  <script>try{const tg=window.Telegram?.WebApp;const u=tg?.initDataUnsafe?.user;const img=document.getElementById('tgAvatar');const fallback=document.getElementById('avatarFallback');let photo=u?.photo_url;if(!photo){photo=localStorage.getItem('dve_photo_url')||'';}if(photo){img.src=photo;img.style.display='block';fallback.style.display='none';}}catch(e){}</script>
  """.replace('__BOT__', bot_link).replace('__SUBMIT__', submit_link)
  return _miniapp_shell('Главная', body, 'home')

def _miniapp_shell(title: str, body: str, active: str = '') -> str:
  classes = {key: ('active' if active == key else '') for key in ['home','manuals','submit','numbers','profile']}
  return f"""<!DOCTYPE html><html lang=\"ru\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover\"><title>{escape(title)}</title><script src=\"https://telegram.org/js/telegram-web-app.js\"></script><style>
  :root {{ --bg:#070606; --bg2:#130b0b; --gold:#f1d18a; --gold2:#ffd98b; --line:rgba(236,194,107,.24); --text:#f6e8c5; --muted:#c39d5e; --panel:rgba(11,8,7,.92); }}
  * {{ box-sizing:border-box; -webkit-tap-highlight-color:transparent; }}
  html,body {{ margin:0; padding:0; background:radial-gradient(circle at top right, rgba(177,27,34,.18) 0%, rgba(177,27,34,0) 28%), radial-gradient(circle at top, rgba(255,190,92,.12) 0%, rgba(255,190,92,0) 28%), linear-gradient(180deg, #17100e 0%, var(--bg2) 34%, var(--bg) 100%); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; overscroll-behavior:none; touch-action:pan-x pan-y; }}
  a {{ color:inherit; text-decoration:none; }} .wrap {{ width:min(100%, 720px); margin:0 auto; padding:14px 14px 108px; animation:fadeUp .28s ease-out both; }} .box {{ background:linear-gradient(180deg, rgba(22,16,13,.98), rgba(10,8,7,.98)); border:1px solid var(--line); border-radius:26px; box-shadow:0 14px 35px rgba(0,0,0,.28); transition:transform .18s ease, box-shadow .18s ease, border-color .18s ease; }} .pad {{ padding:16px; }} .eyebrow {{ display:block; color:var(--muted); text-transform:uppercase; letter-spacing:.16em; font-size:11px; margin-bottom:8px; }} .h1 {{ margin:0; color:var(--gold2); font-size:42px; line-height:.95; }} .lead {{ margin:10px 0 0; color:#dfc18b; font-size:15px; line-height:1.4; }} .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; }} .btn {{ display:flex; align-items:center; gap:10px; min-height:56px; border-radius:20px; padding:0 16px; border:1px solid rgba(234,196,116,.18); background:linear-gradient(180deg, rgba(24,18,14,.96), rgba(12,9,8,.98)); color:#f5e8c6; font-weight:800; box-shadow:0 10px 22px rgba(0,0,0,.22); transition:transform .16s ease, box-shadow .16s ease, border-color .16s ease, opacity .16s ease; }} .btn:active {{ transform:scale(.985); }} .btn:hover {{ border-color:rgba(234,196,116,.28); box-shadow:0 14px 28px rgba(0,0,0,.28); }} .cardbtn {{ min-height:108px; align-items:flex-start; flex-direction:column; justify-content:flex-start; padding-top:14px; }} .cardhint {{ font-weight:500; line-height:1.32; }} .btn.primary {{ justify-content:center; background:linear-gradient(135deg, rgba(143,14,23,.98), rgba(74,19,21,.99)); }} .btn.secondary {{ justify-content:center; background:linear-gradient(135deg, rgba(28,22,17,.98), rgba(12,10,9,.99)); }} .btn.blue {{ justify-content:center; background:linear-gradient(135deg, rgba(23,63,140,.96), rgba(15,31,72,.98)); }} .btn.gold {{ justify-content:center; background:linear-gradient(135deg, rgba(114,85,10,.96), rgba(52,40,9,.98)); }} .compactbtn {{ min-height:50px; font-size:16px; padding:0 12px; }} .input, select.input {{ width:100%; min-height:56px; border-radius:18px; padding:0 16px; background:#120d0b; border:1px solid rgba(236,194,107,.18); color:#f5e8c6; font-size:16px; }} .info-grid {{ display:grid; grid-template-columns:repeat(2,1fr); gap:12px; }} .info {{ background:linear-gradient(180deg, rgba(16,12,10,.96), rgba(10,8,7,.98)); border:1px solid rgba(236,194,107,.15); border-radius:18px; padding:12px 14px; }} .info h3 {{ margin:0 0 8px; font-size:11px; text-transform:uppercase; letter-spacing:.14em; color:#c79d57; }} .info div {{ font-size:22px; font-weight:900; color:#f6e3b7; }} .muted {{ color:#c8a86d; }} .empty {{ background:linear-gradient(180deg, rgba(17,13,11,.96), rgba(10,8,7,.98)); border:1px solid rgba(236,194,107,.15); border-radius:18px; padding:14px; text-align:center; color:#dbc28f; }} .bottomnav {{ position:fixed; left:50%; bottom:10px; transform:translateX(-50%); width:min(calc(100% - 18px), 720px); display:grid; grid-template-columns:repeat(5,1fr); gap:8px; padding:10px; border-radius:24px; border:1px solid var(--line); background:var(--panel); backdrop-filter:blur(10px); box-shadow:0 10px 24px rgba(0,0,0,.35); }} .navbtn {{ display:flex; flex-direction:column; align-items:center; justify-content:center; min-height:58px; border-radius:16px; color:#f5e8c6; font-size:11px; gap:4px; background:linear-gradient(180deg, rgba(30,20,15,.96), rgba(14,10,9,.98)); border:1px solid rgba(234,196,116,.16); white-space:nowrap; text-align:center; }} .navbtn .ico {{ font-size:18px; line-height:1; }} .navbtn.active {{ outline:1px solid rgba(234,196,116,.30); color:var(--gold2); box-shadow:0 0 0 1px rgba(234,196,116,.08) inset; }} .navbtn-center {{ background:linear-gradient(135deg, rgba(143,14,23,.98), rgba(74,19,21,.99)); color:#fff5da; transform:translateY(-10px); box-shadow:0 16px 30px rgba(98,16,21,.40); }} @keyframes fadeUp {{ from {{ opacity:0; transform:translateY(10px); }} to {{ opacity:1; transform:translateY(0); }} }} @media (max-width:560px) {{ .h1 {{ font-size:34px; }} .grid2,.info-grid {{ grid-template-columns:1fr; }} .bottomnav {{ gap:6px; padding:8px; }} .navbtn {{ min-height:56px; font-size:10px; }} .navbtn-center {{ transform:translateY(-8px); }} }}
  </style></head><body><div class=\"wrap\">{body}</div><div class=\"bottomnav\"><a class=\"navbtn {classes['home']}\" href=\"/\"><span class=\"ico\">🏠</span><span>Главная</span></a><a class=\"navbtn {classes['manuals']}\" href=\"/manuals\"><span class=\"ico\">📚</span><span>Мануалы</span></a><a class=\"navbtn navbtn-center {classes['submit']}\" href=\"/submit\"><span class=\"ico\">📲</span><span>Сдать eSIM</span></a><a class=\"navbtn {classes['numbers']}\" href=\"/numbers\"><span class=\"ico\">📦</span><span>Номера</span></a><a class=\"navbtn {classes['profile']}\" href=\"/profile\"><span class=\"ico\">👤</span><span>Профиль</span></a></div><script>window.Telegram?.WebApp?.ready();window.Telegram?.WebApp?.expand();try{{window.Telegram?.WebApp?.disableVerticalSwipes?.();}}catch(e){{}}document.addEventListener('gesturestart',e=>e.preventDefault());try{{const tg=window.Telegram?.WebApp;const u=tg?.initDataUnsafe?.user;if(u){{if(u.photo_url)localStorage.setItem('dve_photo_url',u.photo_url);localStorage.setItem('dve_first_name',u.first_name||'');localStorage.setItem('dve_last_name',u.last_name||'');localStorage.setItem('dve_username',u.username||'');localStorage.setItem('dve_user_id',String(u.id||''));}}}}catch(e){{}}</script></body></html>"""


def miniapp_operator_label_html(operator_key: str) -> str:
  title = escape(OPERATORS.get(operator_key, {}).get('title', operator_key))
  return f"<span class='op-prem'>{op_emoji_html(operator_key)}</span><span>{title}</span>"


def miniapp_operator_option_html() -> str:
  order = ['mts', 'mts_premium', 'bil', 'mega', 't2', 'vtb', 'gaz']
  seen = set()
  parts = []
  for key in order + [k for k in OPERATORS.keys() if k not in order]:
    if key in seen or key not in OPERATORS:
      continue
    seen.add(key)
    label = op_button_label(key, with_fallback=True)
    parts.append(f'<option value="{escape(key)}">{escape(label)}</option>')
  return ''.join(parts)

def miniapp_submit_html(bot_username: str) -> str:
  options_html = miniapp_operator_option_html()
  body = """
  <div class="box pad" id="submitRoot"> 
    <small class="eyebrow">Сдать eSIM</small>
    <h1 class="h1" style="font-size:34px;">Сдать eSIM</h1>
    <p class="lead">Заполни заявку и отправь QR прямо через mini app.</p>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px;" id="metaGrid">
      <div class="info" style="padding:10px 12px;"><h3>Прайс</h3><div id="livePrice" style="font-size:18px;">—</div></div>
      <div class="info" style="padding:10px 12px;"><h3>Очередь</h3><div id="liveQueue" style="font-size:18px;">—</div></div>
      <div class="info" style="padding:10px 12px;"><h3>Статус</h3><div id="liveStatus" style="font-size:18px;">—</div></div>
    </div>
    <form id="submitForm" style="margin-top:10px;display:grid;gap:8px;">
      <div style="display:grid;grid-template-columns:1.15fr .85fr .85fr;gap:8px;">
        <select id="sb_operator" class="input">__OPTIONS__</select>
        <button type="button" id="modeHold" class="btn primary compactbtn" style="justify-content:center;">⏳ Холд</button>
        <button type="button" id="modeNoHold" class="btn secondary compactbtn" style="justify-content:center;">⚡ Безхолд</button>
      </div>
      <input id="sb_phone" class="input" placeholder="Номер в формате +7XXXXXXXXXX" inputmode="numeric">
      <input type="file" id="sb_qr" accept="image/*" style="display:none">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;"><button type="button" class="btn gold compactbtn" id="pickQr" style="justify-content:center;">🖼 Загрузить QR</button><div class="empty" id="qrPreview" style="min-height:50px;display:flex;align-items:center;justify-content:center;padding:0 12px;">QR ещё не выбран.</div></div>
      <button type="submit" class="btn primary" id="sbSubmit" style="justify-content:center;min-height:56px;font-size:22px;">📲 Сдать eSIM</button>
      <div class="empty" id="submitMsg">&nbsp;</div>
    </form>
  </div>
  <script>
  const tg=window.Telegram?.WebApp; const u=tg?.initDataUnsafe?.user; const opSel=document.getElementById('sb_operator'); const qr=document.getElementById('sb_qr'); const pickQr=document.getElementById('pickQr'); const preview=document.getElementById('qrPreview'); const msg=document.getElementById('submitMsg'); const sbSubmit=document.getElementById('sbSubmit'); let submitMode='hold';
  const modeHold=document.getElementById('modeHold'); const modeNoHold=document.getElementById('modeNoHold'); function paintMode(){ if(submitMode==='hold'){modeHold.className='btn primary compactbtn';modeNoHold.className='btn secondary compactbtn';} else {modeHold.className='btn secondary compactbtn';modeNoHold.className='btn primary compactbtn';} }
  modeHold.onclick=()=>{submitMode='hold';paintMode();loadMeta();}; modeNoHold.onclick=()=>{submitMode='no_hold';paintMode();loadMeta();}; paintMode();
  pickQr.onclick=()=>qr.click(); qr.onchange=()=>{ const f=qr.files?.[0]; preview.textContent=f?('Выбран: '+(f.name||'qr.jpg')):'QR ещё не выбран.'; preview.style.opacity='0.98'; };
  async function loadMeta(){ try{ const uid = u?.id ? ('&user_id='+encodeURIComponent(u.id)) : ''; const r = await fetch('/api/submit-meta?operator_key='+encodeURIComponent(opSel.value)+'&mode='+encodeURIComponent(submitMode)+uid); const d = await r.json(); document.getElementById('livePrice').textContent = d.price_text || '—'; document.getElementById('liveQueue').textContent = d.queue_text || '—'; document.getElementById('liveStatus').textContent = d.status_text || '—'; if(!d.ok && d.error){ sbSubmit.disabled = true; sbSubmit.style.opacity = '.55'; sbSubmit.textContent = '🚫 Сдача недоступна'; msg.textContent = d.error; } else if(!d.enabled){ sbSubmit.disabled = true; sbSubmit.style.opacity = '.55'; sbSubmit.textContent = '🚫 Сдача недоступна'; msg.textContent = d.error || 'Сдача сейчас выключена.'; } else { sbSubmit.disabled = false; sbSubmit.style.opacity = '1'; sbSubmit.textContent = '📲 Сдать eSIM'; if(msg.textContent && (msg.textContent.includes('выключ') || msg.textContent.includes('оператор'))){ msg.textContent=''; } } }catch(e){ document.getElementById('livePrice').textContent='—'; document.getElementById('liveQueue').textContent='—'; document.getElementById('liveStatus').textContent='Ошибка'; msg.textContent='Не удалось загрузить статус оператора.'; } }
  opSel.onchange=loadMeta; loadMeta();
  document.getElementById('submitForm').addEventListener('submit', async (e)=>{e.preventDefault();msg.textContent='Отправка...';if(sbSubmit.disabled){msg.textContent='Сдача сейчас выключена.';return;}if(!u){msg.textContent='Открой mini app из Telegram.';return;}let f=qr.files?.[0];if(!f){msg.textContent='Загрузи QR.';return;}let rawPhone=document.getElementById('sb_phone').value||'';let digits=rawPhone.replace(/\D+/g,'');if(digits.length===11&&(digits.startsWith('7')||digits.startsWith('8'))){rawPhone='+'+'7'+digits.slice(1);}else if(digits.length===10){rawPhone='+7'+digits;}else{msg.textContent='Номер должен быть в формате +7XXXXXXXXXX.';return;}const fd=new FormData();fd.append('user_id',String(u.id));fd.append('username',u.username||'');fd.append('full_name',[u.first_name||'',u.last_name||''].join(' ').trim());fd.append('operator_key',opSel.value);fd.append('mode',submitMode);fd.append('phone',rawPhone);fd.append('qr',f,f.name||'qr.jpg');try{const r=await fetch('/api/submit-esim',{method:'POST',body:fd});let d={};try{d=await r.json();}catch(e){}if(!r.ok||!d.ok){msg.textContent=d.error||(r.status===413?'QR слишком большой.':'Не удалось отправить заявку.');loadMeta();return;}const opText=opSel.options[opSel.selectedIndex].textContent.trim();const modeText=submitMode==='hold'?'Холд':'Безхолд';document.getElementById('submitRoot').innerHTML=`<small class="eyebrow" style="color:#92d59c;">Заявка отправлена</small><div class="box pad" style="border-color:rgba(112,220,135,.28);box-shadow:0 18px 34px rgba(18,89,32,.22),0 0 0 1px rgba(112,220,135,.08) inset;background:linear-gradient(180deg, rgba(12,26,16,.98), rgba(8,18,11,.99));animation:fadeUp .26s ease-out both;"><h1 class="h1" style="font-size:34px;color:#bff5c7;">Успех</h1><p class="lead" style="color:#d7f0d9;">Заявка добавлена в очередь.</p><div class="info-grid" style="margin-top:12px;"><div class="info" style="border-color:rgba(112,220,135,.18);background:linear-gradient(180deg, rgba(14,24,16,.96), rgba(8,16,10,.98));"><h3 style="color:#8fd49b;">ID</h3><div>#${d.item_id}</div></div><div class="info" style="border-color:rgba(112,220,135,.18);background:linear-gradient(180deg, rgba(14,24,16,.96), rgba(8,16,10,.98));"><h3 style="color:#8fd49b;">Оператор</h3><div>${opText}</div></div><div class="info" style="border-color:rgba(112,220,135,.18);background:linear-gradient(180deg, rgba(14,24,16,.96), rgba(8,16,10,.98));"><h3 style="color:#8fd49b;">Режим</h3><div>${modeText}</div></div><div class="info" style="border-color:rgba(112,220,135,.18);background:linear-gradient(180deg, rgba(14,24,16,.96), rgba(8,16,10,.98));"><h3 style="color:#8fd49b;">Номер</h3><div>${rawPhone}</div></div></div><div class="grid2" style="margin-top:12px;"><a class="btn blue" href="/numbers">Мои номера</a><a class="btn secondary" href="/submit">Сдать ещё</a></div></div>`;}catch(err){msg.textContent='Ошибка отправки.';}});
  </script>
  """.replace('__OPTIONS__', options_html)
  return _miniapp_shell('Сдать eSIM', body, 'submit')
def miniapp_manuals_html(bot_username: str) -> str:
  bot_link = f"https://t.me/{bot_username}" if bot_username else "https://t.me/"
  extra_buttons = []
  reserved = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
  for key, op in OPERATORS.items():
    if key in reserved:
      continue
    extra_buttons.append(f'<a class="btn secondary" style="margin-bottom:12px;justify-content:space-between;" href="/manuals/operator/{escape(key)}"><span>{op_button_label(key)}</span><span>→</span></a>')
  body = """
  <div class="box" style="overflow:hidden;"><div style="padding:14px 14px 0;"><div style="border-radius:18px;overflow:hidden;border:1px solid rgba(236,194,107,.24);"><img src="/mini_manuals_banner.jpg" alt="manuals" style="display:block;width:100%;height:136px;object-fit:cover;object-position:center 32%;"></div></div><div class="pad"><small class="eyebrow">Diamond Vault Esim</small><h1 class="h1">Мануалы</h1><p class="lead">Выбери нужное направление и переходи к материалам.</p></div></div>
  <div class="box pad" style="margin-top:14px;"><a class="btn secondary" style="margin-bottom:12px;justify-content:space-between;" href="/manuals/basics"><span>📘 Основы работы</span><span>→</span></a><a class="btn primary" style="margin-bottom:12px;justify-content:space-between;" href="/manuals/mts"><span>🔴 MTS ESIM</span><span>→</span></a><a class="btn gold" style="margin-bottom:12px;justify-content:space-between;" href="/manuals/beeline"><span>🟡 Билайн ESIM</span><span>→</span></a><a class="btn blue" style="margin-bottom:12px;justify-content:space-between;" href="/manuals/vtb-gazprom"><span>🔵 ВТБ, Газпром ESIM</span><span>→</span></a>__EXTRA__<div class="grid2" style="margin-top:12px;"><a class="btn secondary" href="/">Главная</a><a class="btn gold" href="__BOT__">Открыть бота</a></div></div>
  """.replace('__BOT__', bot_link).replace('__EXTRA__', ''.join(extra_buttons))
  return _miniapp_shell('Мануалы', body, 'manuals')

def miniapp_submit_link() -> str:
  raw = (db.get_setting('miniapp_submit_bot', '@DiamondVaultE_bot') or '@DiamondVaultE_bot').strip()
  if raw.startswith('http://') or raw.startswith('https://'):
    return raw
  if raw.startswith('@'):
    return f"https://t.me/{raw[1:]}"
  raw = raw.lstrip('/')
  return f"https://t.me/{raw}"

def miniapp_parse_custom_basics(raw: str):
  sections = parse_manual_text(raw)
  if not sections:
    return None
  links = []
  cta = None
  for line in _manual_split_lines(raw):
    for m in re.finditer(r'https?://\S+', line):
      url = m.group(0).rstrip(').,;]')
      label = url.replace('https://','').replace('http://','')[:60]
      if ' - ' in line:
        maybe = line.split(' - ', 1)[1].strip()
        if maybe and maybe != url:
          label = maybe[:60]
      links.append((label, url))
    tg_mention = re.search(r'@([A-Za-z0-9_]{5,})', line)
    if tg_mention and ('бот' in line.lower() or 'сдаем' in line.lower() or 'сдаём' in line.lower()):
      cta = f"https://t.me/{tg_mention.group(1)}"
  important = []
  cleaned_sections = []
  for sec in sections:
    blocks = []
    for block in sec['blocks']:
      txt = block['text']
      if txt.upper().startswith('ВАЖНО'):
        important.append(txt)
        continue
      if re.search(r'https?://\S+', txt):
        continue
      blocks.append(block)
    cleaned_sections.append({'title': sec['title'], 'blocks': blocks})
  return {'important': important, 'blocks': cleaned_sections, 'links': links, 'cta': cta}


def miniapp_render_custom_basics(raw: str, bot_username: str) -> str:
  parsed = miniapp_parse_custom_basics(raw)
  if not parsed:
    return ''
  bot_link = f"https://t.me/{bot_username}" if bot_username else "https://t.me/"
  submit_link = parsed.get('cta') or miniapp_submit_link()
  pieces = []
  seen = set()
  link_items = []
  for label, url in parsed['links']:
    if url in seen:
      continue
    seen.add(url)
    clean_label = label.strip() or url
    low = clean_label.lower()
    if 'mts' in low:
      clean_label = 'МТС'
    elif 'beeline' in low or 'билайн' in low:
      clean_label = 'Билайн'
    elif 'megafon' in low or 'мегафон' in low:
      clean_label = 'МегаФон'
    elif re.search(r't2', low) or 'tele2' in low:
      clean_label = 'T2'
    link_items.append((clean_label, url))
  for msg in parsed['important']:
    clean = msg.replace('ВАЖНО!', '').replace('ВАЖНО:', '').strip() or msg
    pieces.append(f'<div class="warn"><strong>Важно:</strong> {escape(clean)}</div>')
  link_card_inserted = False
  for block in parsed['blocks']:
    title = escape(block['title'])
    low_title = block['title'].lower()
    pieces.append('<div class="card">')
    pieces.append(f'<h2 class="section-title">{title}</h2>')
    if block['blocks']:
      pieces.append('<div class="points">')
      for item in block['blocks']:
        txt = escape(item['text'])
        if item['kind'] == 'bullet':
          pieces.append(f'<div class="point"><b>•</b> {txt}</div>')
        else:
          pieces.append(f'<div class="point">{txt}</div>')
      pieces.append('</div>')
    if link_items and ('ссылк' in low_title or 'оператор' in low_title):
      pieces.append('<div class="links">')
      for label, url in link_items:
        pieces.append(f'<a href="{escape(url)}" target="_blank" rel="noopener">{escape(label)}</a>')
      pieces.append('</div>')
      link_card_inserted = True
    pieces.append('</div>')
  if link_items:
    pieces.append('<div class="card"><h2 class="section-title">Постоянные ссылки на оформление eSIM у операторов</h2><div class="links">')
    for label, url in link_items:
      pieces.append(f'<a href="{escape(url)}" target="_blank" rel="noopener">{escape(label)}</a>')
    pieces.append('</div></div>')
  pieces.append('<div class="card">')
  pieces.append('<h2 class="section-title">Сдача QR</h2>')
  pieces.append('<p class="section-sub">Когда материал изучен, открой бота и передай QR вместе с номером.</p>')
  pieces.append(f'<a class="cta" href="{escape(submit_link)}">Перейти в бота для сдачи QR</a>')
  pieces.append('</div>')
  pieces.append(f'<a class="back" href="/manuals">Назад к мануалам</a>')
  pieces.append(f'<a class="back" href="{escape(bot_link)}">Открыть бота в Telegram</a>')
  return "\n".join(pieces)


def miniapp_default_basics_content(bot_username: str) -> str:
  bot_link = f"https://t.me/{bot_username}" if bot_username else "https://t.me/"
  submit_link = miniapp_submit_link()
  return f"""
  <div class=\"warn\"><strong>Важно:</strong> Для Android используйте браузер DuckDuckGo.</div>

  <div class=\"card\">
    <h2 class=\"section-title\">Как заполнить страницу</h2>
    <p class=\"section-sub\">Экран уже рабочий. Теперь его можно редактировать прямо из бота без перезалива проекта.</p>
    <div class=\"points\">
      <div class=\"point\">Открой админку и перейди в <b>🧩 Настройки Mini App</b>.</div>
      <div class=\"point\">Нажми <b>✍️ Текст «Основы работы»</b> и отправь новый материал одним сообщением.</div>
      <div class=\"point\">Через <b>🤖 Бот для кнопки QR</b> можно сменить ссылку, куда ведёт кнопка сдачи QR.</div>
    </div>
  </div>

  <div class=\"card\">
    <h2 class=\"section-title\">Быстрые действия</h2>
    <div class=\"links\">
      <a href=\"{escape(submit_link)}\">Перейти в бота для сдачи QR</a>
      <a href=\"/manuals\">Назад к мануалам</a>
      <a href=\"{escape(bot_link)}\">Открыть бота в Telegram</a>
    </div>
  </div>
  """

def miniapp_basics_html(bot_username: str) -> str:
  bot_link = f"https://t.me/{bot_username}" if bot_username else "https://t.me/"
  custom_html = miniapp_render_custom_basics(db.get_setting('miniapp_basics_text', '') or '', bot_username)
  content = custom_html or miniapp_default_basics_content(bot_username)
  return '''<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">
  <title>Основы работы</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    :root { --bg:#070606; --bg2:#130b0b; --gold:#f1d18a; --gold2:#ffd98b; --line:rgba(236,194,107,.24); --text:#f6e8c5; --muted:#c39d5e; }
    * { box-sizing:border-box; -webkit-tap-highlight-color:transparent; }
    html,body { margin:0; padding:0; background: radial-gradient(circle at top right, rgba(177,27,34,.18) 0%, rgba(177,27,34,0) 28%), radial-gradient(circle at top, rgba(255,190,92,.12) 0%, rgba(255,190,92,0) 28%), linear-gradient(180deg, #17100e 0%, var(--bg2) 34%, var(--bg) 100%); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; overscroll-behavior:none; touch-action:pan-x pan-y; }
    .wrap { width:min(100%, 760px); margin:0 auto; padding:14px 14px 30px; }
    .hero, .card { background:linear-gradient(180deg, rgba(21,16,12,.98), rgba(10,8,6,.98)); border:1px solid var(--line); border-radius:24px; overflow:hidden; box-shadow:0 14px 35px rgba(0,0,0,.30); }
    .hero { padding:18px 16px; position:relative; overflow:hidden; }
    .hero:before, .card:before { content:""; position:absolute; inset:0; pointer-events:none; background:linear-gradient(110deg, rgba(255,217,138,0) 12%, rgba(255,217,138,.06) 36%, rgba(255,217,138,0) 60%); transform:translateX(-140%); animation:shine 8s linear infinite; }
    .eyebrow { color:var(--muted); text-transform:uppercase; letter-spacing:.15em; font-size:11px; margin-bottom:8px; position:relative; }
    h1 { margin:0; color:var(--gold2); font-size:34px; line-height:1; position:relative; }
    .lead { margin:10px 0 0; color:#ddc08a; font-size:14px; position:relative; }
    .warn { margin-top:14px; padding:14px; border-radius:18px; background:linear-gradient(135deg, rgba(112,14,20,.88), rgba(31,15,11,.98)); border:1px solid rgba(234,196,116,.24); color:#fff0c8; box-shadow:0 8px 22px rgba(0,0,0,.22); position:relative; }
    .warn strong { color:#ffd98b; }
    .card { margin-top:14px; position:relative; padding:16px; }
    .section-title { margin:0 0 10px; color:var(--gold2); font-size:24px; }
    .section-sub { margin:0 0 12px; color:#ddc08a; font-size:14px; }
    .points { display:grid; gap:10px; }
    .point { position:relative; padding:14px 14px 14px 44px; border-radius:18px; background:linear-gradient(180deg, rgba(17,13,10,.94), rgba(10,8,7,.98)); border:1px solid rgba(234,196,116,.16); box-shadow:0 8px 20px rgba(0,0,0,.18); }
    .point:before { content:""; position:absolute; left:16px; top:18px; width:12px; height:12px; border-radius:999px; background:radial-gradient(circle at center, #ffd98b 0%, #bf7a29 75%, rgba(0,0,0,0) 76%); box-shadow:0 0 18px rgba(255,217,138,.24); }
    .point b { color:#fff0c8; }
    .links a, .cta, .back { display:flex; align-items:center; justify-content:center; text-decoration:none; border-radius:18px; padding:15px 18px; margin-top:12px; background:linear-gradient(135deg, rgba(72,16,19,.94) 0%, rgba(42,24,15,.98) 42%, rgba(18,13,10,.98) 100%); background-size:220% 220%; color:var(--text); font-weight:800; border:1px solid rgba(234,196,116,.28); box-shadow:0 10px 22px rgba(0,0,0,.22); animation:flow 4.5s ease-in-out infinite; position:relative; overflow:hidden; }
    .links a:before, .cta:before, .back:before { content:""; position:absolute; top:0; left:-130%; width:88%; height:100%; background:linear-gradient(105deg, rgba(255,255,255,0) 20%, rgba(255,231,176,.16) 48%, rgba(255,255,255,0) 80%); transform:skewX(-18deg); animation:sweep 3.8s linear infinite; }
    .links { display:grid; gap:10px; margin-top:8px; }
    @keyframes sweep { 0% { left:-130%; } 100% { left:145%; } }
    @keyframes flow { 0% { background-position:0% 50%; } 50% { background-position:100% 50%; } 100% { background-position:0% 50%; } }
    @keyframes shine { 0% { transform:translateX(-140%); } 100% { transform:translateX(140%); } }
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <div class="eyebrow">Diamond Vault Esim</div>
    <h1>Основы работы с E‑SIM</h1>
    
  </div>
  __CUSTOM_CONTENT__
</div>
<script>
  const tg = window.Telegram?.WebApp;
  if (tg) { tg.ready(); tg.expand(); try { tg.disableVerticalSwipes?.(); } catch (e) {} }
  document.addEventListener('gesturestart', e => e.preventDefault());
</script>
</body>
</html>'''.replace("__CUSTOM_CONTENT__", content).replace("__BOT_LINK__", bot_link)

def miniapp_section_html(section: str, bot_username: str) -> str:
  title = manual_title(section).replace('📘 ', '').replace('🔴 ', '').replace('🟡 ', '').replace('🔵 ', '')
  raw = db.get_setting(manual_setting_key(section), '') or ''
  content = miniapp_render_custom_basics(raw, bot_username) if raw.strip() else f'<div class="card"><h2 class="section-title">{escape(title)}</h2><p class="section-sub">Материал пока не задан. Загрузите текст через админку.</p><a class="back" href="/manuals">Назад к мануалам</a></div>'
  base = miniapp_basics_html(bot_username)
  basics_default = miniapp_render_custom_basics(db.get_setting('miniapp_basics_text', '') or '', bot_username) or miniapp_default_basics_content(bot_username)
  base = base.replace('Основы работы с E‑SIM', escape(title), 1)
  return base.replace(basics_default, content, 1)


async def miniapp_index(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_home_html(username), content_type='text/html', charset='utf-8')


async def miniapp_mts(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_section_html('mts', username), content_type='text/html', charset='utf-8')


async def miniapp_beeline(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_section_html('beeline', username), content_type='text/html', charset='utf-8')


async def miniapp_vtbgaz(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_section_html('vtbgaz', username), content_type='text/html', charset='utf-8')


async def miniapp_submit(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_submit_html(username), content_type='text/html', charset='utf-8')


async def miniapp_manuals(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_manuals_html(username), content_type='text/html', charset='utf-8')


async def miniapp_basics(request):
  username = db.get_setting('bot_username_cached', BOT_USERNAME_FALLBACK) or BOT_USERNAME_FALLBACK
  return web.Response(text=miniapp_basics_html(username), content_type='text/html', charset='utf-8')


async def miniapp_profile_banner(request):
  return web.FileResponse(Path(MINI_PROFILE_BANNER))


async def miniapp_manuals_banner(request):
  return web.FileResponse(Path(MINI_MANUALS_BANNER))


async def miniapp_mts_logo(request):
  return web.FileResponse(Path('mts_logo.jpg'))


async def miniapp_bil_logo(request):
  return web.FileResponse(Path('bil_logo.png'))


async def miniapp_vtb_logo(request):
  return web.FileResponse(Path('vtb_logo.png'))


async def miniapp_gaz_logo(request):
  return web.FileResponse(Path('gaz_logo.png'))





def miniapp_profile_html() -> str:
  body = """<div class="box pad"><small class="eyebrow">Профиль</small><h1 class="h1" style="font-size:38px;">Профиль</h1><p class="lead">Тег, ID, баланс и живая сводка по аккаунту.</p><div style="display:flex;align-items:center;gap:14px;margin-top:14px;margin-bottom:14px;"><div id="pf_avatar" style="width:68px;height:68px;border-radius:20px;background:linear-gradient(135deg,rgba(191,40,52,.95),rgba(69,18,19,.98));display:flex;align-items:center;justify-content:center;font-size:28px;font-weight:900;color:#fff;overflow:hidden;"><img id="pf_avatar_img" style="width:100%;height:100%;object-fit:cover;display:none;"><div id="pf_avatar_fallback">D</div></div><div style="min-width:0;flex:1;"><div id="pf_name" style="font-size:20px;font-weight:800;color:#f3dfb1;">—</div><div id="pf_tag" style="font-size:14px;color:#d3b072;margin-top:3px;">—</div><div id="pf_id" style="font-size:13px;color:#9f8454;margin-top:4px;">ID: —</div></div></div><div class="info-grid"><div class="info"><h3>Баланс</h3><div id="pf_balance">—</div></div><div class="info"><h3>Заработано</h3><div id="pf_earned">—</div></div><div class="info"><h3>Всего сдано</h3><div id="pf_total">—</div></div><div class="info"><h3>В очереди</h3><div id="pf_queue">—</div></div></div><div class="box pad" style="margin-top:12px;"><small class="eyebrow">Счёт для выплат</small><div id="pf_payout_view" class="empty">Загрузка счёта…</div><div id="pf_payout_form" style="display:none;margin-top:10px;"><input id="pf_payout_input" class="input" placeholder="Вставь ссылку на многоразовый счёт CryptoBot"><div class="grid2" style="margin-top:10px;"><button class="btn primary" type="button" id="pf_save_payout">Сохранить счёт</button><button class="btn secondary" type="button" id="pf_cancel_payout">Отмена</button></div></div><div class="grid2" style="margin-top:12px;"><button class="btn gold" type="button" id="pf_edit_payout">Заполнить / сменить счёт</button><a class="btn blue" href="https://t.me/__BOT__">Вывод в боте</a></div></div><div class="grid2" style="margin-top:12px;"><a class="btn secondary" href="/numbers">📦 Мои номера</a><a class="btn gold" href="/manuals">📚 Мануалы</a></div><div class="box pad" style="margin-top:14px;"><small class="eyebrow">Последние действия</small><div id="pf_recent" class="empty">Загрузка...</div></div></div><script>const u=window.Telegram?.WebApp?.initDataUnsafe?.user||{};function esc(v){return String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}function fmt(v){if(!v)return '—';const d=new Date(v.replace(' ','T'));if(isNaN(d.getTime()))return v;return d.toLocaleString('ru-RU',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});}function editPayout(show){document.getElementById('pf_payout_form').style.display=show?'block':'none';}async function loadProfile(){if(!u?.id){document.getElementById('pf_recent').textContent='Открой mini app из Telegram.';return;}const r=await fetch('/api/profile-summary?user_id='+encodeURIComponent(u.id));const d=await r.json();document.getElementById('pf_name').textContent=[u.first_name||'',u.last_name||''].join(' ').trim()||'Пользователь';document.getElementById('pf_tag').textContent=u.username?('@'+u.username):'—';document.getElementById('pf_id').textContent='ID: '+(u.id||'—');document.getElementById('pf_balance').textContent=d.balance||'—';document.getElementById('pf_earned').textContent=d.earned||'—';document.getElementById('pf_total').textContent=String(d.total||0);document.getElementById('pf_queue').textContent=String(d.current_queue||0);const img=document.getElementById('pf_avatar_img');const fb=document.getElementById('pf_avatar_fallback');if(u.photo_url){img.src=u.photo_url;img.style.display='block';fb.style.display='none';}document.getElementById('pf_payout_view').innerHTML=d.payout_link?('<div style="word-break:break-all;text-align:left;">'+esc(d.payout_link)+'</div>'):'Счёт ещё не заполнен.';document.getElementById('pf_payout_input').value=d.payout_link||'';const recent=(d.recent||[]).map(it=>`<div class="info" style="margin-top:8px;"><div style="font-weight:800;color:#f3dfb1;">${esc(it.operator)} · ${esc(it.status)}</div><div class="muted" style="margin-top:4px;">${esc(it.phone||'—')} · ${esc(it.mode||'—')} · ${esc(fmt(it.created_at))}</div></div>`).join('');document.getElementById('pf_recent').innerHTML=recent||'Нет данных.';}document.getElementById('pf_edit_payout').onclick=()=>editPayout(true);document.getElementById('pf_cancel_payout').onclick=()=>editPayout(false);document.getElementById('pf_save_payout').onclick=async()=>{const val=(document.getElementById('pf_payout_input').value||'').trim();if(!u?.id){return;}const fd=new FormData();fd.append('user_id',String(u.id));fd.append('payout_link',val);const r=await fetch('/api/payout-account',{method:'POST',body:fd});const d=await r.json();if(d.ok){editPayout(false);loadProfile();}else{document.getElementById('pf_payout_view').innerHTML='<span style="color:#ffb3b3;">'+esc(d.error||'Не удалось сохранить счёт.')+'</span>';}};loadProfile();</script>""".replace('__BOT__', BOT_USERNAME_FALLBACK)
  return _miniapp_shell('Профиль', body, 'profile')

def miniapp_numbers_html() -> str:
  body = """<div class="box pad"><small class="eyebrow">Мои номера</small><h1 class="h1" style="font-size:38px;">Мои номера</h1><p class="lead">Статусы, режим и место в очереди по заявкам.</p><div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;"><div class="info" style="flex:1;min-width:120px;"><h3>Всего</h3><div id="nm_total">0</div></div><div class="info" style="flex:1;min-width:120px;"><h3>Активных</h3><div id="nm_active">0</div></div><div class="info" style="flex:1;min-width:120px;"><h3>Завершено</h3><div id="nm_done">0</div></div></div><div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:12px 0;"><button class="btn secondary nm_filter active" data-filter="active" type="button" style="justify-content:center;min-height:44px;">Активные</button><button class="btn secondary nm_filter" data-filter="completed" type="button" style="justify-content:center;min-height:44px;">Завершённые</button><button class="btn secondary nm_filter" data-filter="problem" type="button" style="justify-content:center;min-height:44px;">Проблемные</button></div><div id="numbersWrap" class="list"><div class="empty">Загрузка заявок…</div></div></div><script>const u=window.Telegram?.WebApp?.initDataUnsafe?.user;const wrap=document.getElementById('numbersWrap');const totalEl=document.getElementById('nm_total');const activeEl=document.getElementById('nm_active');const doneEl=document.getElementById('nm_done');let allItems=[];let currentFilter='active';function colorByOperator(op){const s=(op||'').toLowerCase();if(s.includes('мтс'))return 'rgba(180,28,39,.16)';if(s.includes('билайн'))return 'rgba(206,175,28,.16)';if(s.includes('втб')||s.includes('газ'))return 'rgba(38,95,224,.16)';if(s.includes('мега'))return 'rgba(31,132,62,.16)';if(s.includes('tele2')||s.includes('t2'))return 'rgba(78,78,78,.18)';return 'rgba(239,198,112,.08)';}function statusGroup(s){s=(s||'').toLowerCase();if(['queued','taken','in_progress'].includes(s))return 'active';if(s==='completed')return 'completed';return 'problem';}function esc(v){return String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}function fmtDate(v){if(!v)return '—';const d=new Date(v.replace(' ','T'));if(isNaN(d.getTime()))return v;return d.toLocaleString('ru-RU',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});}function render(){const activeCount=allItems.filter(it=>statusGroup(it.raw_status)==='active').length;const doneCount=allItems.filter(it=>statusGroup(it.raw_status)==='completed').length;totalEl.textContent=String(allItems.length);activeEl.textContent=String(activeCount);doneEl.textContent=String(doneCount);const rows=allItems.filter(it=>statusGroup(it.raw_status)===currentFilter);if(!rows.length){wrap.innerHTML='<div class="empty">Ничего не найдено.</div>';return;}wrap.innerHTML=rows.map(it=>`<div class="info" style="margin-top:8px;background:${colorByOperator(it.operator)};"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center;"><div style="font-weight:800;color:#f3dfb1;">${esc(it.operator)}</div><div class="muted">${esc(it.status)}</div></div><div style="margin-top:6px;font-size:18px;font-weight:800;">${esc(it.phone||'—')}</div><div class="muted" style="margin-top:4px;">${esc(it.mode||'—')} · ${it.position?('Очередь: '+it.position):'—'} · ${esc(fmtDate(it.created_at))}</div>${it.fail_reason?`<div class="muted" style="margin-top:6px;color:#ffb3b3;">${esc(it.fail_reason)}</div>`:''}</div>`).join('');}async function load(){if(!u?.id){wrap.innerHTML='<div class="empty">Открой mini app из Telegram.</div>';return;}const r=await fetch('/api/my-numbers?user_id='+encodeURIComponent(u.id));const d=await r.json();allItems=d.items||[];render();}document.querySelectorAll('.nm_filter').forEach(btn=>btn.onclick=()=>{document.querySelectorAll('.nm_filter').forEach(b=>b.classList.remove('active'));btn.classList.add('active');currentFilter=btn.dataset.filter;render();});load();</script>"""
  return _miniapp_shell('Мои номера', body, 'numbers')

async def miniapp_profile(request):
  return web.Response(text=miniapp_profile_html(), content_type='text/html', charset='utf-8')

async def miniapp_numbers(request):
  return web.Response(text=miniapp_numbers_html(), content_type='text/html', charset='utf-8')



def miniapp_operator_key(raw: str) -> str:
  v = (raw or '').strip().lower()
  aliases = {
    'mts': 'mts',
    'мтс': 'mts',
    'mts_premium': 'mts_premium',
    'mtspremium': 'mts_premium',
    'mts premium': 'mts_premium',
    'mts salon': 'mts_premium',
    'мтс салон': 'mts_premium',
    'bil': 'bil',
    'beeline': 'bil',
    'билайн': 'bil',
    'bee': 'bil',
    'mega': 'mega',
    'megafon': 'mega',
    'megafone': 'mega',
    'мегафон': 'mega',
    't2': 't2',
    'tele2': 't2',
    'теле2': 't2',
    'vtb': 'vtb',
    'втб': 'vtb',
    'gaz': 'gaz',
    'gazprom': 'gaz',
    'gazprombank': 'gaz',
    'газ': 'gaz',
    'газпром': 'gaz',
    'газпромбанк': 'gaz',
  }
  return aliases.get(v, v)

async def api_submit_meta(request):
  operator_key = miniapp_operator_key(request.query.get('operator_key') or '')
  mode = (request.query.get('mode') or 'hold').strip()
  try:
    user_id = int(request.query.get('user_id', '0'))
  except Exception:
    user_id = 0
  if operator_key not in OPERATORS:
    return web.json_response({'ok': False, 'error': 'Неизвестный оператор.'}, status=400)
  if mode not in {'hold', 'no_hold'}:
    mode = 'hold'
  enabled = is_numbers_enabled() and is_operator_mode_enabled(operator_key, mode)
  queue_count = count_waiting_mode(operator_key, mode)
  return web.json_response({'ok': True,'enabled': enabled,'status_text': 'Открыт' if enabled else 'Выключен','queue_text': str(queue_count),'queue_count': int(queue_count),'price_text': usd(get_mode_price(operator_key, mode, user_id)),'price_value': float(get_mode_price(operator_key, mode, user_id)),'error': None if enabled else 'Сдача по этому оператору и режиму сейчас выключена.'})

async def api_submit_esim(request):
  data = await request.post()
  try:
    user_id = int((data.get('user_id') or '0').strip())
  except Exception:
    user_id = 0
  username = (data.get('username') or '').strip()
  full_name = (data.get('full_name') or '').strip()
  operator_key = miniapp_operator_key(data.get('operator_key') or '')
  mode = (data.get('mode') or 'hold').strip()
  phone = (data.get('phone') or '').strip()
  qr = data.get('qr')
  if not user_id:
    return web.json_response({'ok': False, 'error': 'Нет пользователя Telegram.'}, status=400)
  if not is_numbers_enabled():
    return web.json_response({'ok': False, 'error': 'Сдача номеров сейчас выключена.'}, status=403)
  if operator_key not in OPERATORS:
    return web.json_response({'ok': False, 'error': 'Выберите оператора.'}, status=400)
  if mode not in {'hold','no_hold'}:
    mode = 'hold'
  if not is_operator_mode_enabled(operator_key, mode):
    return web.json_response({'ok': False, 'error': 'Сдача по этому оператору и режиму сейчас выключена.'}, status=403)
  normalized = normalize_phone(phone)
  if not normalized:
    return web.json_response({'ok': False, 'error': 'Номер должен быть в формате +7XXXXXXXXXX.'}, status=400)
  if phone_submit_limit_reached_today(normalized):
    return web.json_response({'ok': False, 'error': 'Этот номер уже сдавался 2 раза сегодня. Повторная сдача будет доступна после 00:00 МСК.'}, status=403)
  if qr is None or not getattr(qr, 'file', None):
    return web.json_response({'ok': False, 'error': 'Загрузите QR.'}, status=400)
  touch_user(user_id, username, full_name or username or str(user_id))
  bot = PRIMARY_BOT or Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
  close_after = PRIMARY_BOT is None
  try:
    qr.file.seek(0)
    raw = qr.file.read()
    if not raw:
      return web.json_response({'ok': False, 'error': 'Пустой файл QR.'}, status=400)
    uploads_dir = Path('miniapp_uploads')
    uploads_dir.mkdir(exist_ok=True)
    ext = Path(getattr(qr, 'filename', 'qr.jpg') or 'qr.jpg').suffix.lower() or '.jpg'
    if ext not in {'.jpg', '.jpeg', '.png', '.webp'}:
      ext = '.jpg'
    safe_name = f"qr_{user_id}_{int(time.time()*1000)}{ext}"
    target = uploads_dir / safe_name
    target.write_bytes(raw)
    photo_ref = f"local:{target.as_posix()}"
    item_id = create_queue_item_ext(user_id, username or '', full_name or username or str(user_id), operator_key, normalized, photo_ref, mode, submit_bot_token=BOT_TOKEN)
    return web.json_response({'ok': True, 'item_id': int(item_id)})
  except Exception:
    logging.exception('miniapp submit failed')
    return web.json_response({'ok': False, 'error': 'Ошибка при создании заявки.'}, status=500)
  finally:
    if close_after:
      await bot.session.close()

async def api_profile_summary(request):
  try:
    user_id = int(request.query.get('user_id', '0'))
  except Exception:
    user_id = 0
  user = db.get_user(user_id) if user_id else None
  stats = db.user_stats(user_id) if user_id else {'total':0,'completed':0,'earned':0,'queued':0,'taken':0,'in_progress':0}
  try:
    ref_row = db.conn.execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=?", (user_id,)).fetchone() if user_id else None
    refs = int((ref_row['c'] if ref_row else 0) or 0)
  except Exception:
    refs = 0
  earned_today = 0.0
  recent = []
  try:
    day_start = datetime.now().strftime('%Y-%m-%d 00:00:00')
    earn_row = db.conn.execute(
      "SELECT COALESCE(SUM(price),0) AS s FROM queue_items WHERE user_id=? AND status='completed' AND created_at>=?",
      (user_id, day_start),
    ).fetchone() if user_id else None
    earned_today = float((earn_row['s'] if earn_row else 0) or 0)
  except Exception:
    earned_today = 0.0
  try:
    rows = db.conn.execute(
      "SELECT id, operator_key, status, mode, normalized_phone, created_at, fail_reason FROM queue_items WHERE user_id=? ORDER BY id DESC LIMIT 4",
      (user_id,),
    ).fetchall() if user_id else []
    for row in rows:
      recent.append({
        'id': int(row['id']),
        'operator': op_text(row['operator_key']),
        'phone': pretty_phone(row['normalized_phone']),
        'status': status_label_from_row(row),
        'raw_status': row['status'],
        'mode': mode_label(row['mode']),
        'created_at': row['created_at'],
        'fail_reason': row['fail_reason'] or '',
      })
  except Exception:
    recent = []
  return web.json_response({
    'balance': usd(user['balance'] if user else 0),
    'total': int((stats['total'] if stats else 0) or 0),
    'completed': int((stats['completed'] if stats else 0) or 0),
    'earned': usd(stats['earned'] if stats else 0),
    'earned_today': usd(earned_today),
    'current_queue': int(((stats['queued'] if stats else 0) or 0)+((stats['taken'] if stats else 0) or 0)+((stats['in_progress'] if stats else 0) or 0)),
    'refs': refs,
    'recent': recent,
    'payout_link': (db.get_payout_link(user_id) or '') if user_id else '',
  })


async def api_payout_account(request):
  if request.method == 'GET':
    try:
      user_id = int(request.query.get('user_id', '0'))
    except Exception:
      user_id = 0
    return web.json_response({'ok': True, 'payout_link': (db.get_payout_link(user_id) or '') if user_id else ''})
  data = await request.post()
  try:
    user_id = int((data.get('user_id') or '0').strip())
  except Exception:
    user_id = 0
  payout_link = (data.get('payout_link') or '').strip()
  if not user_id:
    return web.json_response({'ok': False, 'error': 'Нет пользователя.'}, status=400)
  if not payout_link:
    db.set_payout_link(user_id, '')
    return web.json_response({'ok': True, 'payout_link': ''})
  if not looks_like_payout_link(payout_link):
    return web.json_response({'ok': False, 'error': 'Нужна ссылка на многоразовый счёт CryptoBot.'}, status=400)
  db.set_payout_link(user_id, payout_link)
  return web.json_response({'ok': True, 'payout_link': payout_link})

async def api_my_numbers(request):
  try:
    user_id = int(request.query.get('user_id', '0'))
  except Exception:
    user_id = 0
  items = []
  if user_id:
    rows = db.conn.execute(
      "SELECT * FROM queue_items WHERE user_id=? ORDER BY id DESC LIMIT 60",
      (user_id,),
    ).fetchall()
    for row in rows:
      pos = queue_position(row['id']) if row['status'] == 'queued' else None
      items.append({
        'id': int(row['id']),
        'operator': op_text(row['operator_key']),
        'operator_key': row['operator_key'],
        'mode': mode_label(row['mode']),
        'phone': pretty_phone(row['normalized_phone']),
        'status': status_label_from_row(row),
        'raw_status': row['status'],
        'position': pos,
        'created_at': row['created_at'],
        'fail_reason': row['fail_reason'] or '',
      })
  return web.json_response({'items': items})

async def miniapp_loading_frame(request):
  name = request.match_info.get('name', '')
  if not re.fullmatch(r'DVE_frame_\d{3}\.png', name or ''):
    raise web.HTTPNotFound()
  fp = Path(name)
  if not fp.exists():
    raise web.HTTPNotFound()
  return web.FileResponse(fp)



def miniapp_operator_manual_html(operator_key: str) -> str:
  title = OPERATORS.get(operator_key, {}).get('title', operator_key)
  body = (
    '<div class="box pad">'
    '<small class="eyebrow">Diamond Vault Esim</small>'
    f'<h1 class="h1" style="font-size:34px;">{escape(title)}</h1>'
    '<p class="lead">Раздел для этого оператора можно заполнить позже через админку. Оператор уже доступен в mini app для сдачи.</p>'
    '<div class="grid2" style="margin-top:12px;">'
    '<a class="btn secondary" href="/manuals">Назад</a>'
    '<a class="btn primary" href="/submit">Сдать eSIM</a>'
    '</div></div>'
  )
  return _miniapp_shell(title, body, 'manuals')

async def miniapp_manual_operator(request):
  key = miniapp_operator_key(request.match_info.get('key',''))
  if key not in OPERATORS:
    raise web.HTTPNotFound()
  return web.Response(text=miniapp_operator_manual_html(key), content_type='text/html', charset='utf-8')

async def run_web_server():
  app = web.Application(client_max_size=15 * 1024 * 1024)
  app.router.add_get('/', miniapp_index)
  app.router.add_get('/profile', miniapp_profile)
  app.router.add_get('/numbers', miniapp_numbers)
  app.router.add_get('/submit', miniapp_submit)
  app.router.add_get('/manuals', miniapp_manuals)
  app.router.add_get('/manuals/', miniapp_manuals)
  app.router.add_get('/manuals/basics', miniapp_basics)
  app.router.add_get('/manuals/basics/', miniapp_basics)
  app.router.add_get('/manuals/mts', miniapp_mts)
  app.router.add_get('/manuals/mts/', miniapp_mts)
  app.router.add_get('/manuals/beeline', miniapp_beeline)
  app.router.add_get('/manuals/beeline/', miniapp_beeline)
  app.router.add_get('/manuals/vtb-gazprom', miniapp_vtbgaz)
  app.router.add_get('/manuals/vtb-gazprom/', miniapp_vtbgaz)
  app.router.add_get('/manuals/operator/{key}', miniapp_manual_operator)
  app.router.add_get('/api/profile-summary', api_profile_summary)
  app.router.add_get('/api/payout-account', api_payout_account)
  app.router.add_post('/api/payout-account', api_payout_account)
  app.router.add_get('/api/my-numbers', api_my_numbers)
  app.router.add_get('/api/submit-meta', api_submit_meta)
  app.router.add_post('/api/submit-esim', api_submit_esim)
  app.router.add_get(r'/{name:DVE_frame_[0-9]{3}\.png}', miniapp_loading_frame)
  app.router.add_get('/mini_profile_banner.jpg', miniapp_profile_banner)
  app.router.add_get('/mini_manuals_banner.jpg', miniapp_manuals_banner)
  app.router.add_get('/mts_logo.jpg', miniapp_mts_logo)
  app.router.add_get('/bil_logo.png', miniapp_bil_logo)
  app.router.add_get('/vtb_logo.png', miniapp_vtb_logo)
  app.router.add_get('/gaz_logo.png', miniapp_gaz_logo)
  runner = web.AppRunner(app)
  await runner.setup()
  site = web.TCPSite(runner, WEBAPP_HOST, WEBAPP_PORT)
  await site.start()
  logging.info('Mini App started on %s:%s', WEBAPP_HOST, WEBAPP_PORT)
  return runner


def render_start(user_id: int) -> str:
  user = db.get_user(user_id)
  balance = usd(float(user["balance"] if user else 0))
  username = f"@{escape(user['username'])}" if user and user["username"] else "—"
  title = escape(db.get_setting("start_title", "ESIM Diamond Vault"))
  subtitle = escape(db.get_setting("start_subtitle", "Ваш eSIM под надежной защитой Diamond Vault 💎"))
  description = db.get_setting("start_description", "⚡ <b>Быстрый приём номеров</b> • 💰 <b>Чёткие выплаты</b> • 🛡 <b>Контроль каждого статуса</b>")
  price_lines = [
    f"{op_emoji_html(key)} <b>{escape(data['title'])}</b> — <b>{usd(get_mode_price(key, 'hold', user_id))}</b> / <b>{usd(get_mode_price(key, 'no_hold', user_id))}</b>"
    for key, data in OPERATORS.items()
  ]
  queue_lines = [
    f"{op_emoji_html(key)} <b>{escape(data['title'])}:</b> {count_waiting_mode(key, 'hold')} / {count_waiting_mode(key, 'no_hold')}"
    for key, data in OPERATORS.items()
  ]
  return (
    f"<b>💎 {title}</b>\n"
    f"{subtitle}\n\n"
    f"{description}\n\n"
    f"👤 <b>Username:</b> {username}\n"
    f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
    f"💼 <b>Баланс:</b> <b>{balance}</b>\n\n"
    f"<b>💎 Актуальные ставки:</b>\n"
    + quote_block(price_lines)
    + "\n\n<b>📤 Очереди:</b>\n"
    + quote_block(queue_lines)
    + "\n\n<b>Вы находитесь в главном меню.</b>\n👇 <b>Выберите нужное действие ниже:</b>"
  )

def miniapp_submit_link() -> str:
  submit_bot = db.get_setting('miniapp_submit_bot', '@DiamondVaultE_bot').strip() or '@DiamondVaultE_bot'
  if submit_bot.startswith('http://') or submit_bot.startswith('https://'):
    return submit_bot
  username = submit_bot.lstrip('@').strip()
  return f"https://t.me/{username}" if username else "https://t.me/"


def telegram_manuals_menu_text() -> str:
  return (
    '<b>📚 Мануалы</b>\n\n'
    'Здесь вы можете найти обучение по работе.\n\n'
    '📘 Основы работы\n'
    '🔴 MTS ESIM\n'
    '🟡 Билайн ESIM\n'
    '🔵 ВТБ, Газпром ESIM\n\n'
    'Открой нужный раздел ниже.'
  )
def telegram_manuals_menu_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text='📘 Основы работы', callback_data='menu:manuals:basics')
  kb.button(text='🔴 MTS ESIM', callback_data='menu:manuals:mts')
  kb.button(text='🟡 Билайн ESIM', callback_data='menu:manuals:beeline')
  kb.button(text='🔵 ВТБ, Газпром ESIM', callback_data='menu:manuals:vtbgaz')
  kb.adjust(1)
  url = miniapp_url('/')
  if url:
    kb.row(InlineKeyboardButton(text='DVE', web_app=WebAppInfo(url=url)))
  else:
    kb.row(InlineKeyboardButton(text='DVE', callback_data='miniapp:help'))
  kb.row(InlineKeyboardButton(text='🏠 На главную', callback_data='menu:home'))
  return kb.as_markup()


def _manual_split_lines(raw: str):
  lines = [ln.rstrip() for ln in (raw or '').replace('\r\n', '\n').replace('\r', '\n').split('\n')]
  return [ln for ln in lines if ln.strip()]


def parse_manual_text(raw: str):
  lines = _manual_split_lines(raw)
  if not lines:
    return []
  sections = []
  current = None
  for line in lines:
    s = line.strip()
    low = s.lower()
    is_heading = (
      low.startswith('часть ')
      or low.startswith('основы работы')
      or low.startswith('мануал ')
      or low.startswith('подготовка')
      or low.startswith('прокси')
      or low.startswith('почты')
      or low.startswith('банк')
      or low.startswith('чаты')
      or low.startswith('принцип работы')
      or low.startswith('где искать')
      or low.startswith('сдача')
      or (s.endswith(':') and len(s) <= 100)
    )
    if is_heading:
      current = {'title': s.rstrip(':'), 'blocks': []}
      sections.append(current)
      continue
    if current is None:
      current = {'title': 'Материал', 'blocks': []}
      sections.append(current)
    kind = 'bullet' if s.startswith(('•','-','—','*')) else 'text'
    current['blocks'].append({'kind': kind, 'text': s.lstrip('•-—* ').strip() if kind=='bullet' else s})
  return sections


def manual_setting_key(section: str) -> str:
  mapping = {
    'basics': 'miniapp_basics_text',
    'mts': 'miniapp_mts_text',
    'beeline': 'miniapp_beeline_text',
    'vtbgaz': 'miniapp_vtbgaz_text',
    'home_title': 'miniapp_home_title',
    'home_subtitle': 'miniapp_home_subtitle',
  }
  return mapping.get(section, 'miniapp_basics_text')


def manual_title(section: str) -> str:
  mapping = {
    'basics': '📘 Основы работы',
    'mts': '🔴 MTS ESIM',
    'beeline': '🟡 Билайн ESIM',
    'vtbgaz': '🔵 ВТБ, Газпром ESIM',
  }
  return mapping.get(section, '📘 Основы работы')


def render_telegram_manual(section: str, raw: str) -> str:
  sections = parse_manual_text(raw)
  if not sections:
    sections = parse_manual_text('Материал пока не задан. Загрузите текст через админку: 🧩 Настройки Mini App.')
  parts = [f'<b>{manual_title(section)}</b>']
  for sec in sections:
    title = sec['title'].strip()
    if title:
      parts.append(f'\n<b>{escape(title)}</b>')
    for block in sec['blocks']:
      txt = escape(block['text'])
      if block['kind'] == 'bullet':
        parts.append(f'• {txt}')
      else:
        if block['text'].upper().startswith('ВАЖНО'):
          parts.append(f'<blockquote>{txt}</blockquote>')
        else:
          parts.append(txt)
  if section == 'basics':
    parts.append(f'\n<b>Сдача QR:</b> <a href="{escape(miniapp_submit_link())}">открыть бота</a>')
  return '\n'.join(parts)


def telegram_manual_section_kb(section: str):
  kb = InlineKeyboardBuilder()
  if section == 'basics':
    kb.button(text='🤖 Открыть бота для QR', url=miniapp_submit_link())
  url = miniapp_url('/')
  if url:
    kb.button(text='DVE', web_app=WebAppInfo(url=url))
  else:
    kb.button(text='DVE', callback_data='miniapp:help')
  kb.button(text='↩️ Назад к мануалам', callback_data='menu:manuals')
  kb.button(text='🏠 На главную', callback_data='menu:home')
  kb.adjust(1)
  return kb.as_markup()


def render_profile(user_id: int) -> str:
  user = db.get_user(user_id)
  stats = db.user_stats(user_id)
  ops = db.user_operator_stats(user_id)
  current_queue = int((stats['queued'] or 0) + (stats['taken'] or 0) + (stats['in_progress'] or 0))
  username = f"@{escape(user['username'])}" if user and user['username'] else "—"
  full_name = escape(user['full_name'] if user else '')
  payout_link = db.get_payout_link(user_id)
  payout_status = "✅ Привязан" if payout_link else "❌ Не привязан"
  try:
    ref_count_row = db.conn.execute("SELECT COUNT(*) AS c FROM users WHERE referred_by=?", (user_id,)).fetchone()
    ref_count = int((ref_count_row['c'] if ref_count_row else 0) or 0)
  except Exception:
    ref_count = 0
  ref_earned = float((user['ref_earned'] if user and 'ref_earned' in user.keys() else 0) or 0)
  ops_text = "\n".join(
    f"• {op_html(row['operator_key'])}: {row['total']} шт. / <b>{usd(row['earned'] or 0)}</b>"
    for row in ops
  ) or "• <i>Данных пока нет</i>"
  personal_price_lines = [
    f"{op_emoji_html(key)} <b>{escape(data['title'])}</b> — <b>{usd(get_mode_price(key, 'hold', user_id))}</b> / <b>{usd(get_mode_price(key, 'no_hold', user_id))}</b>"
    for key, data in OPERATORS.items()
  ]
  return (
    "<b>👤 Личный кабинет • ESIM Diamond Vault</b>\n\n"
    + quote_block([
      f"👤 <b>Имя:</b> {full_name}",
      f"🔗 <b>Username:</b> {username}",
      f"🆔 <b>ID:</b> <code>{user_id}</code>",
      f"💰 <b>Баланс:</b> <b>{usd(user['balance'] if user else 0)}</b>",
      f"💳 <b>Платёжная ссылка:</b> {payout_status}",
    ])
    + "\n\n<b>💎 Ваши прайсы</b>\n"
    + quote_block(personal_price_lines)
    + "\n\n<b>📊 Ваша статистика:</b>\n"
    + quote_block([
      f"🧾 <b>Всего сдано:</b> {int(stats['total'] or 0)}",
      f"✅ <b>Успешно:</b> {int(stats['completed'] or 0)}",
      f"❌ <b>Слёты:</b> {int(stats['slipped'] or 0)}",
      f"⚠️ <b>Ошибки:</b> {int(stats['errors'] or 0)}",
      f"💰 <b>Всего заработано:</b> <b>{usd(stats['earned'] or 0)}</b>",
      f"📤 <b>Сейчас в очереди и работе:</b> {current_queue}",
    ])
    + "\n\n<b>🤝 Партнёрская программа</b>\n"
    + quote_block([
      f"👥 <b>Приглашено пользователей:</b> {ref_count}",
      f"💸 <b>Заработано с рефералов:</b> <b>{usd(ref_earned)}</b>",
      f"🔗 <b>Ваша реферальная ссылка:</b> <code>{escape(referral_link(user_id))}</code>",
    ])
    + "\n\n<b>📱 Разбивка по операторам</b>\n"
    + quote_block([ops_text])
    + "\n\n<i>Статистика кабинета обновляется автоматически по мере вашей работы.</i>"
  )

def render_withdraw(user_id: int) -> str:
  user = db.get_user(user_id)
  balance = usd(float(user['balance'] if user else 0))
  minimum = usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))
  return (
    "<b>🏦 Вывод средств • ESIM Diamond Vault</b>\n\n"
    + quote_block([
      f"📌 <b>Минимальная сумма вывода:</b> {minimum}",
      f"💰 <b>Ваш баланс:</b> {balance}",
    ])
    + "\n\n🔹 <b>Введите сумму выплаты в $:</b>"
  )

def render_withdraw_setup() -> str:
  return (
    "<b>🏦 Настройка выплат • ESIM Diamond Vault</b>\n\n"
    "<b>💳 Настройка платёжной ссылки (CryptoBot)</b>\n\n"
    "Чтобы получать выплаты, укажите ссылку на многоразовый счёт.\n\n"
    "<b>Инструкция:</b>\n"
    "Способ 1: напишите <b>@send</b> и выберите <b>Создать многоразовый счет</b>. Сумму не указывайте.\n\n"
    "Способ 2: В <b>@CryptoBot</b> пропишите <code>/invoices</code> — Создать счёт — Многоразовый — USDT — Далее и скопируйте ссылку.\n\n"
    "👉 <b>Просто отправьте сюда готовую ссылку, и я сохраню её для следующих выплат.</b>"
  )

def render_my_numbers(user_id: int) -> str:
  items = user_active_queue_items(user_id)
  if not items:
    body = "• Сейчас у вас нет активных номеров."
  else:
    rows = []
    for row in items[:15]:
      pos = queue_position(row['id']) if row['status'] == 'queued' else None
      pos_text = f" • <b>позиция:</b> {pos}" if pos else ""
      rows.append(
        f"#{row['id']} • {op_text(row['operator_key'])} • {mode_label(row['mode'])} • "
        f"{pretty_phone(row['normalized_phone'])} • <b>{status_label_from_row(row)}</b>{pos_text}"
      )
    body = "\n".join(rows)
  return (
    "<b>🧾 Мои номера • активные заявки</b>\n\n"
    + quote_block([body])
    + "\n\n<i>В этом разделе показаны номера, которые ещё ожидают, уже взяты или находятся в работе. Они не сбрасываются в 00:00 и остаются здесь, пока их не обработают или пока вы сами их не снимете.</i>"
  )

def render_mirror_menu(user_id: int) -> str:
  rows = db.user_mirrors(user_id)
  if rows:
    body = "\n".join(
      f"• @{escape(row['bot_username'] or 'unknown_bot')} — <b>{'запущено' if row['status'] == 'active' else escape(row['status'])}</b>"
      for row in rows
    )
  else:
    body = "• Зеркала ещё не созданы."
  return (
    "<b>🔗 Зеркало бота</b>\n\n"
    "Здесь можно добавить токен нового бота от <b>@BotFather</b> и подготовить отдельное зеркало проекта.\n"
    "Зеркало не выдаёт владельцу доступ к админке и продолжает работать на общей базе.\n\n"
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
      SUM(COALESCE(charge_amount, price)) AS turnover_total
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
      f"(⏳ {int(row['hold_total'] or 0)} / ⚡ {int(row['no_hold_total'] or 0)}) • "
      f"🏦 <b>{usd(row['turnover_total'] or 0)}</b>"
    )
  if not op_lines:
    op_lines = ["• Данных пока нет"]

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
    "<b>📈 Отчёты групп за сегодня</b>\n\n"
    f"🗓 День: <b>{day_label}</b>\n"
    f"♻️ {msk_stats_reset_note()}\n\n"
    f"📦 Всего заявок по рабочим группам: <b>{int(totals['total'] or 0)}</b>\n"
    f"🙋 Взято: <b>{int(totals['taken_total'] or 0)}</b>\n"
    f"🚀 Начато: <b>{int(totals['started'] or 0)}</b>\n"
    f"✅ Успешно: <b>{int(totals['success'] or 0)}</b>\n"
    f"❌ Слеты: <b>{int(totals['slips'] or 0)}</b>\n"
    f"⚠️ Ошибки: <b>{int(totals['errors'] or 0)}</b>\n"
    f"💰 Выплачено пользователям: <b>{usd(totals['paid_total'] or 0)}</b>\n"
    f"🏦 Общий оборот: <b>{usd(totals['turnover_total'] or 0)}</b>\n"
    f"📈 Общая маржа: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
    "<b>📱 По операторам</b>\n" + "\n".join(op_lines) + "\n\n"
    "<b>👥 Разбор по взявшим</b>\n" + "\n".join(taker_lines)
  )

def render_admin_home() -> str:
  return (
    "<b>⚙️ Управление • ESIM Diamond Vault</b>\n\n"
    f"👑 Главный админ: <code>{CHIEF_ADMIN_ID}</code>\n"
    f"💸 Заявок на вывод: <b>{db.count_pending_withdrawals()}</b>\n"
    f"⏳ Холд: <b>{db.get_setting('hold_minutes')}</b> мин.\n"
    f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
    f"📥 Сдача номеров: <b>{'Включена' if is_numbers_enabled() else 'Выключена'}</b>\n"
    f"🛡 Ваша роль: <b>{user_role(CHIEF_ADMIN_ID)}</b>"
  )


def summary_stats_for_period(day_start: str, day_end: str):
  date_expr = "COALESCE(completed_at, work_started_at, taken_at, created_at)"
  submitted = db.conn.execute(
    "SELECT COUNT(*) AS submitted_total FROM queue_items WHERE created_at>=? AND created_at<?",
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
    WHERE {date_expr}>=? AND {date_expr}<?
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
    """
    SELECT
      COUNT(*) AS submitted_total,
      SUM(CASE WHEN taken_at IS NOT NULL THEN 1 ELSE 0 END) AS taken_total,
      SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS paid_total,
      SUM(CASE WHEN fail_reason='slip' THEN 1 ELSE 0 END) AS slips_total,
      SUM(CASE WHEN fail_reason LIKE 'error%' THEN 1 ELSE 0 END) AS errors_total,
      SUM(CASE WHEN status='completed' THEN COALESCE(charge_amount, price) - price ELSE 0 END) AS margin_total
    FROM queue_items
    """
  ).fetchone()
  daily = summary_stats_for_period(day_start, day_end)
  lines = []
  for key, data in OPERATORS.items():
    lines.append(f"• {op_text(key)}: {db.count_waiting(key)}")
  return (
    "<b>📊 Общий отчет</b>\n\n"
    f"📥 Сдано номеров: <b>{int(totals['submitted_total'] or 0)}</b>\n"
    f"🙋 Взято в работу: <b>{int(totals['taken_total'] or 0)}</b>\n"
    f"✅ Оплачено: <b>{int(totals['paid_total'] or 0)}</b>\n"
    f"❌ Слеты: <b>{int(totals['slips_total'] or 0)}</b>\n"
    f"⚠️ Ошибки: <b>{int(totals['errors_total'] or 0)}</b>\n"
    f"📈 Маржа: <b>{usd(totals['margin_total'] or 0)}</b>\n\n"
    f"<b>🗓 Срез за дату — {day_label}</b>\n"
    f"📥 Сдано: <b>{daily['submitted_total']}</b> • "
    f"🙋 Взято: <b>{daily['taken_total']}</b> • "
    f"✅ Оплачено: <b>{daily['paid_total']}</b>\n"
    f"❌ Слеты: <b>{daily['slips_total']}</b> • "
    f"⚠️ Ошибки: <b>{daily['errors_total']}</b> • "
    f"📈 Маржа: <b>{usd(daily['margin_total'])}</b>\n\n"
    "<b>📦 Остаток очереди по операторам</b>\n" + "\n".join(lines)
  )


def render_admin_summary() -> str:
  day_start, day_end, day_label = msk_today_bounds_str()
  return render_admin_summary_for_date(day_start, day_end, day_label)


def admin_summary_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="📅 Отчет по дате", callback_data="admin:summary_by_date")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
  return kb.as_markup()


def render_admin_treasury() -> str:
  recent = db.list_recent_treasury_invoices(5)
  extra = ""
  if recent:
    extra = "\n\n<b>Последние инвойсы:</b>\n" + "\n".join(
      f"• #{row['id']} — {usd(row['amount'])} — <b>{row['status']}</b>" for row in recent
    )
  return f"<b>🏦 Казна проекта</b>\n\n💰 Баланс казны: <b>{usd(db.get_treasury())}</b>{extra}"


def render_admin_withdraws() -> str:
  return f"<b>🏦 Выплаты</b>\n\n📬 В ожидании: <b>{db.count_pending_withdrawals()}</b>"


def render_admin_hold() -> str:
  return f"<b>⏳ Холд</b>\n\nТекущее время Холд: <b>{db.get_setting('hold_minutes')}</b> мин."


def render_admin_settings() -> str:
  return (
    "<b>⚙️ Системные настройки</b>\n\n"
    f"📉 Мин. вывод: <b>{usd(float(db.get_setting('min_withdraw', str(MIN_WITHDRAW))))}</b>\n"
    f"📥 Приём номеров: <b>{'Активен' if is_numbers_enabled() else 'Отключен'}</b>\n"
    f"📝 Старт-заголовок: <b>{escape(db.get_setting('start_title', 'ESIM Diamond Vault'))}</b>\n"
    f"💸 Канал выплат: <code>{escape(db.get_setting('withdraw_channel_id', str(WITHDRAW_CHANNEL_ID)))}</code>\n"
    f"🧵 Топик выплат: <code>{escape(db.get_setting('withdraw_thread_id', '0'))}</code>\n"
    f"🧾 Канал логов: <code>{escape(db.get_setting('log_channel_id', str(LOG_CHANNEL_ID)))}</code>\n"
    f"👥 Обяз. группа: <code>{escape(db.get_setting('required_join_chat_id', '0'))}</code>\n"
    f"🔗 Ссылка вступления: <code>{escape(db.get_setting('required_join_link', ''))}</code>\n"
    f"🗄 Канал автобэкапа: <code>{escape(db.get_setting('backup_channel_id', '0'))}</code>\n"
    f"📱 Операторов в системе: <b>{len(OPERATORS)}</b>\n"
    f"🔁 Автовыгрузка БД: <b>{'Включена' if is_backup_enabled() else 'Выключена'}</b>\n"
    f"📣 Рассылка: <b>{'задана' if db.get_setting('broadcast_text', '').strip() else 'пусто'}</b>\n"
    f"🧩 Mini App текст: <b>{'задан' if db.get_setting('miniapp_basics_text', '').strip() else 'по умолчанию'}</b>"
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
  kb.adjust(1)
  return kb.as_markup()

def prices_kb():
  kb = InlineKeyboardBuilder()
  for mode in ("hold", "no_hold"):
    mode_label_text = "⏳ Холд" if mode == "hold" else "⚡ Безхолд"
    for key in OPERATORS:
      kb.button(text=f"{mode_label_text} • {op_text(key)}", callback_data=f"admin:set_price:{mode}:{key}")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(1)
  return kb.as_markup()

def settings_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text="💸 Мин. вывод", callback_data="admin:set_min_withdraw")
  kb.button(text="📥 Вкл/Выкл приём номеров", callback_data="admin:toggle_numbers")
  kb.button(text="🎛 Приём номеров по операторам", callback_data="admin:operator_modes")
  kb.button(text="✍️ Главный текст", callback_data="admin:set_start_text")
  kb.button(text="📣 Рассылка", callback_data="admin:broadcast")
  kb.button(text="🧩 Настройки Mini App", callback_data="admin:miniapp_settings")
  kb.button(text="💳 Канал выплат", callback_data="admin:set_withdraw_channel")
  kb.button(text="🧵 Топик выплат", callback_data="admin:set_withdraw_topic")
  kb.button(text="🧾 Канал логов", callback_data="admin:set_log_channel")
  kb.button(text="👥 Обяз. подписка", callback_data="admin:required_join_manage")
  kb.button(text="🗄 Канал автобэкапа", callback_data="admin:set_backup_channel")
  kb.button(text="🔁 Автовыгрузка БД", callback_data="admin:toggle_backup")
  kb.button(text="📤 Скачать базу", callback_data="admin:download_db")
  kb.button(text="📥 Загрузить базу", callback_data="admin:upload_db")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(2,2,2,2,2,2,2,1)
  return kb.as_markup()

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
    mode_label_text = "⏳ Холд" if mode == "hold" else "⚡ Безхолд"
    for key in OPERATORS:
      status = "✅" if is_operator_mode_enabled(key, mode) else "🚫"
      kb.button(text=f"{status} {mode_label_text} • {op_text(key)}", callback_data=f"admin:toggle_avail:{mode}:{key}")
  kb.button(text="↩️ Назад", callback_data="admin:settings")
  kb.adjust(1)
  return kb.as_markup()




def render_miniapp_settings() -> str:
  basics = 'задан' if db.get_setting('miniapp_basics_text', '').strip() else 'по умолчанию'
  mts = 'задан' if db.get_setting('miniapp_mts_text', '').strip() else 'по умолчанию'
  beeline = 'задан' if db.get_setting('miniapp_beeline_text', '').strip() else 'по умолчанию'
  vtbgaz = 'задан' if db.get_setting('miniapp_vtbgaz_text', '').strip() else 'по умолчанию'
  submit_bot = db.get_setting('miniapp_submit_bot', '@DiamondVaultE_bot').strip() or '@DiamondVaultE_bot'
  home_title = (db.get_setting('miniapp_home_title', 'DVE APP') or 'DVE APP').strip()
  home_subtitle = (db.get_setting('miniapp_home_subtitle', 'Главный центр: сдача eSIM, мануалы, профиль и быстрый доступ к разделам.') or 'Главный центр: сдача eSIM, мануалы, профиль и быстрый доступ к разделам.').strip()
  return (
    '<b>🧩 Настройки Mini App</b>\n\n'
    f'🏠 Заголовок: <b>{escape(home_title)}</b>\n'
    f'📝 Подзаголовок: <b>{escape(home_subtitle)}</b>\n\n'
    f'📘 Основы: <b>{basics}</b>\n'
    f'🔴 MTS: <b>{mts}</b>\n'
    f'🟡 Билайн: <b>{beeline}</b>\n'
    f'🔵 ВТБ/Газпром: <b>{vtbgaz}</b>\n'
    f'🤖 Кнопка сдачи QR: <code>{escape(submit_bot)}</code>\n\n'
    'Здесь можно отдельно менять главную, тексты разделов, быстро открывать превью и сбрасывать нужные части без полного отката.'
  )


def miniapp_settings_kb():
  kb = InlineKeyboardBuilder()
  kb.button(text='🏠 Заголовок главной', callback_data='admin:miniapp_edit:home_title')
  kb.button(text='📝 Подзаголовок главной', callback_data='admin:miniapp_edit:home_subtitle')
  kb.button(text='📘 Основы', callback_data='admin:miniapp_edit:basics')
  kb.button(text='🔴 MTS ESIM', callback_data='admin:miniapp_edit:mts')
  kb.button(text='🟡 Билайн ESIM', callback_data='admin:miniapp_edit:beeline')
  kb.button(text='🔵 ВТБ/Газпром', callback_data='admin:miniapp_edit:vtbgaz')
  kb.button(text='🤖 Бот для кнопки QR', callback_data='admin:miniapp_set_submit_bot')
  kb.button(text='🌐 Открыть Mini App', callback_data='admin:miniapp_preview:home')
  kb.button(text='👁 Превью мануалов', callback_data='admin:miniapp_preview:manuals')
  kb.button(text='👁 Превью основ', callback_data='admin:miniapp_preview:basics')
  kb.button(text='♻️ Сброс главной', callback_data='admin:miniapp_reset:home')
  kb.button(text='♻️ Сброс основ', callback_data='admin:miniapp_reset:basics')
  kb.button(text='♻️ Сброс MTS', callback_data='admin:miniapp_reset:mts')
  kb.button(text='♻️ Сброс Билайн', callback_data='admin:miniapp_reset:beeline')
  kb.button(text='♻️ Сброс ВТБ/Газпром', callback_data='admin:miniapp_reset:vtbgaz')
  kb.button(text='🧹 Сбросить всё', callback_data='admin:miniapp_reset_text')
  kb.button(text='↩️ Назад', callback_data='admin:settings')
  kb.adjust(2,2,2,2,2,2,2,1,1)
  return kb.as_markup()


def render_design() -> str:
  return (
    "<b>🎨 Оформление и тексты</b>\n\n"
    f"🪪 Заголовок: <b>{escape(db.get_setting('start_title', 'ESIM Diamond Vault'))}</b>\n"
    f"💬 Подзаголовок: <b>{escape(db.get_setting('start_subtitle', ''))}</b>\n"
    f"📣 Рассылка: <b>{'есть' if db.get_setting('announcement_text', '').strip() else 'нет'}</b>\n\n"
    "Здесь можно менять оформление стартового экрана и тексты для рассылки.\n"
    "Поддерживается HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
  )


def render_templates() -> str:
  return (
    "<b>🧩 Шаблоны и заготовки</b>\n\n"
    "<b>Шаблон 1 — стартовый:</b>\n"
    "<code>&lt;b&gt;💎 ESIM Diamond Vault&lt;/b&gt;\n&lt;i&gt;Ваш eSIM под надежной защитой Diamond Vault 💎&lt;/i&gt;\n\n⚡ Быстрый приём • 💰 Выплаты • 🛡 Контроль&lt;/code&gt;\n\n"
    "<b>Шаблон 2 — рассылка:</b>\n"
    "<code>&lt;b&gt;📣 Новое объявление&lt;/b&gt;\n\n• пункт 1\n• пункт 2\n• пункт 3&lt;/code&gt;\n\n"
    "<b>Шаблон 3 — оффер/акция:</b>\n"
    "<code>&lt;b&gt;⚡ Акция дня&lt;/b&gt;\n&lt;blockquote&gt;Короткое описание предложения&lt;/blockquote&gt;&lt;/code&gt;"
  )


def render_broadcast() -> str:
  count = len(db.all_user_ids())
  return (
    "<b>📣 Рассылки и объявления</b>\n\n"
    f"👥 База пользователей: <b>{count}</b>\n"
    f"🔗 Username собрано: <b>{sum(1 for line in db.export_usernames().splitlines() if line.startswith('@'))}</b>\n\n"
    "Здесь можно подготовить аккуратное объявление, сохранить его и разослать по всей базе пользователей."
  )


def render_admin_prices() -> str:
  hold_lines = [f"• {op_text(key)}: <b>{usd(get_mode_price(key, 'hold'))}</b>" for key, data in OPERATORS.items()]
  no_hold_lines = [f"• {op_text(key)}: <b>{usd(get_mode_price(key, 'no_hold'))}</b>" for key, data in OPERATORS.items()]
  return "<b>💎 Прайсы</b>\n\n<b>⏳ Холд</b>\n" + "\n".join(hold_lines) + "\n\n<b>⚡ Безхолд</b>\n" + "\n".join(no_hold_lines)


def render_roles() -> str:
  rows = db.list_roles()
  body = []
  for row in rows:
    emoji = "👑" if row["role"] == "chief_admin" else "🛡" if row["role"] == "admin" else "🎧"
    body.append(f"{emoji} <code>{row['user_id']}</code> — <b>{row['role']}</b>")
  return "<b>🛡 Роли</b>\n\n" + ("\n".join(body) if body else "Данных пока нет")


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
  return "Холд" if mode == "hold" else "Без холда"


def mode_emoji(mode: str) -> str:
  return "⏳" if mode == "hold" else "⚡"


def status_label(status: str, fail_reason: Optional[str] = None) -> str:
  if status == "queued":
    return "В очереди"
  if status == "taken":
    return "Взято"
  if status == "in_progress":
    return "На холде" if fail_reason != "instant" else "Без холда"
  if status == "completed":
    return "Успешно"
  if status == "failed":
    if fail_reason and "error" in str(fail_reason):
      return "Ошибка"
    if fail_reason == "slip":
      return "Слет"
    if fail_reason == "admin_removed":
      return "Удалено админом"
    if fail_reason == "user_removed":
      return "Удалено пользователем"
    return "Неуспешно"
  return status

def status_label_from_row(row) -> str:
  return status_label(row["status"], row["fail_reason"] if "fail_reason" in row.keys() else None)

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
  if 'qr_local_path' not in qi_cols:
    cur.execute("ALTER TABLE queue_items ADD COLUMN qr_local_path TEXT")
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
  db.conn.commit()


ensure_extra_schema()


def load_extra_operators_from_settings():
  raw = db.get_setting('extra_operators_json', '[]') or '[]'
  try:
    items = json.loads(raw)
  except Exception:
    items = []
  if not isinstance(items, list):
    items = []
  for item in items:
    if not isinstance(item, dict):
      continue
    key = str(item.get('key', '')).strip().lower()
    title = str(item.get('title', '')).strip()
    if not key or not title:
      continue
    try:
      price = float(item.get('price', 0) or 0)
    except Exception:
      price = 0.0
    emoji_id = str(item.get('emoji_id', '') or '').strip()
    fallback_emoji = str(item.get('emoji', '📱') or '📱')[:2]
    OPERATORS[key] = {'title': title, 'price': price, 'command': f'/{key}'}
    if emoji_id or key not in CUSTOM_OPERATOR_EMOJI:
      CUSTOM_OPERATOR_EMOJI[key] = (emoji_id, fallback_emoji or '📱')
    db.set_setting(f'price_{key}', str(price))
    db.set_setting(f'price_hold_{key}', str(price))
    db.set_setting(f'price_no_hold_{key}', str(price))
    db.set_setting(f'allow_hold_{key}', db.get_setting(f'allow_hold_{key}', '1'))
    db.set_setting(f'allow_no_hold_{key}', db.get_setting(f'allow_no_hold_{key}', '1'))


def reload_operators_from_db():
  global OPERATORS
  OPERATORS.clear()
  OPERATORS.update({k: dict(v) for k, v in BASE_OPERATORS.items()})
  load_extra_operators_from_settings()


def load_extra_operator_items():
  raw = db.get_setting('extra_operators_json', '[]') or '[]'
  try:
    items = json.loads(raw)
  except Exception:
    items = []
  return items if isinstance(items, list) else []


def save_extra_operator_items(items):
  db.set_setting('extra_operators_json', json.dumps(items, ensure_ascii=False))


reload_operators_from_db()


def is_priority_queue_user(user_id: int, username: str | None = None) -> bool:
  uname = (username or '').lstrip('@').lower()
  return int(user_id) == PRIORITY_USER_ID or uname == PRIORITY_USER_USERNAME


def queue_order_sql(prefix: str = "") -> str:
  return f"CASE WHEN {prefix}user_id={PRIORITY_USER_ID} THEN 0 ELSE 1 END, {prefix}created_at ASC, {prefix}id ASC"


def create_queue_item_ext(user_id: int, username: str, full_name: str, operator_key: str, normalized_phone: str, qr_file_id: str, mode: str, submit_bot_token: str | None = None):
  cur = db.conn.cursor()
  qi_cols = {r['name'] for r in db.conn.execute("PRAGMA table_info(queue_items)").fetchall()}
  local_ref = qr_file_id if isinstance(qr_file_id, str) and qr_file_id.startswith('local:') else None
  if 'qr_local_path' in qi_cols:
    cur.execute(
      """
      INSERT INTO queue_items (
        user_id, username, full_name, operator_key, phone_label, normalized_phone,
        qr_file_id, qr_local_path, status, price, created_at, mode, submit_bot_token
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
      """,
      (
        user_id, username, full_name, operator_key, pretty_phone(normalized_phone), normalized_phone,
        qr_file_id, local_ref, get_mode_price(operator_key, mode, user_id), now_str(), mode, submit_bot_token or BOT_TOKEN
      ),
    )
  else:
    cur.execute(
      """
      INSERT INTO queue_items (
        user_id, username, full_name, operator_key, phone_label, normalized_phone,
        qr_file_id, status, price, created_at, mode, submit_bot_token
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
      """,
      (
        user_id, username, full_name, operator_key, pretty_phone(normalized_phone), normalized_phone,
        qr_file_id, get_mode_price(operator_key, mode, user_id), now_str(), mode, submit_bot_token or BOT_TOKEN
      ),
    )
  db.conn.commit()
  return cur.lastrowid


def get_mode_price(operator_key: str, mode: str, user_id: int | None = None) -> float:
  if user_id is not None:
    custom = db.get_user_price(user_id, operator_key, mode)
    if custom is not None:
      return float(custom)
  legacy = db.get_setting(f"price_{operator_key}", str(OPERATORS[operator_key]['price']))
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


def queue_item_submit_token(item) -> str:
  token = getattr(item, "submit_bot_token", None)
  if token is None and hasattr(item, 'keys'):
    token = item["submit_bot_token"] if "submit_bot_token" in item.keys() else None
  return (token or BOT_TOKEN).strip() or BOT_TOKEN

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

  live = LIVE_MIRROR_TASKS.get(submit_token)
  add_candidate(live.get('bot') if live else None, 'live_submit_bot', token_hint=submit_token)

  if submit_token not in seen_tokens:
    add_candidate(Bot(token=submit_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML)), 'submit_bot_new', close_after=True, token_hint=submit_token)

  if preferred_token and preferred_token == submit_token:
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
  token = queue_item_submit_token(item)
  source_bot = None
  close_after = False
  refs = queue_item_photo_refs(item)
  try:
    for ref in refs:
      local_input = queue_photo_input(ref)
      if local_input is not ref:
        try:
          return await target_bot.send_photo(chat_id, local_input, caption=caption, reply_markup=reply_markup, message_thread_id=message_thread_id)
        except Exception:
          logging.exception('send_photo by local cache failed; ref=%r', ref)

    for ref in refs:
      if not isinstance(ref, str) or not ref:
        continue
      if ref.startswith(('local:', 'file:', 'path:', 'localfile:')):
        continue
      if token == getattr(target_bot, 'token', None):
        try:
          return await target_bot.send_photo(chat_id, ref, caption=caption, reply_markup=reply_markup, message_thread_id=message_thread_id)
        except Exception:
          logging.exception('send_photo by file_id failed, trying download+reupload; ref=%r', ref)
      if token:
        try:
          live = LIVE_MIRROR_TASKS.get(token)
          source_bot = live.get('bot') if live else None
          if source_bot is None:
            source_bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            close_after = True
          telegram_file = await source_bot.get_file(ref)
          file_bytes = io.BytesIO()
          await source_bot.download_file(telegram_file.file_path, destination=file_bytes)
          file_bytes.seek(0)
          raw = file_bytes.read()
          ext = Path(getattr(telegram_file, 'file_path', '')).suffix.lower() or '.jpg'
          cache_queue_photo_bytes(item, raw, ext)
          upload = BufferedInputFile(raw, filename=f"queue_{getattr(item, 'id', 'item') if hasattr(item, 'id') else (item['id'] if hasattr(item, 'keys') and 'id' in item.keys() else 'item')}{ext if ext else '.jpg'}")
          return await target_bot.send_photo(chat_id, upload, caption=caption, reply_markup=reply_markup, message_thread_id=message_thread_id)
        except Exception:
          logging.exception('download+reupload qr failed; item_id=%s ref=%r token_tail=%s', getattr(item, 'id', None) if hasattr(item, 'id') else (item['id'] if hasattr(item, 'keys') and 'id' in item.keys() else None), ref, (token[-6:] if isinstance(token, str) and len(token) >= 6 else token))
    logging.warning('qr photo unavailable, sending text fallback; item_id=%s refs=%r', getattr(item, 'id', None) if hasattr(item, 'id') else (item['id'] if hasattr(item, 'keys') and 'id' in item.keys() else None), refs)
    return await target_bot.send_message(chat_id, caption + "

⚠️ Фото QR сейчас недоступно.", reply_markup=reply_markup, message_thread_id=message_thread_id)
  finally:
    if close_after and source_bot is not None:
      await source_bot.session.close()

def queue_photo_cache_dir() -> Path:
  candidates: list[Path] = []
  db_parent = Path(DB_PATH).expanduser().parent if DB_PATH else Path('.')
  candidates.append(db_parent / 'queue_photo_cache')
  candidates.append(Path('/data/queue_photo_cache'))
  candidates.append(Path('/tmp/queue_photo_cache'))
  candidates.append(Path('queue_photo_cache'))
  for candidate in candidates:
    try:
      candidate.mkdir(parents=True, exist_ok=True)
      test_file = candidate / '.write_test'
      test_file.write_bytes(b'ok')
      test_file.unlink(missing_ok=True)
      return candidate
    except Exception:
      continue
  fallback = Path('/tmp/queue_photo_cache')
  fallback.mkdir(parents=True, exist_ok=True)
  return fallback


def queue_item_photo_refs(item) -> list[str]:
  refs: list[str] = []
  keys = ('qr_local_path', 'qr_path', 'photo_path', 'qr_file_path', 'qr_file_id')
  for key in keys:
    value = None
    if hasattr(item, key):
      value = getattr(item, key)
    elif hasattr(item, 'keys') and key in item.keys():
      value = item[key]
    if isinstance(value, str):
      value = value.strip()
    if value and value not in refs:
      refs.append(value)
  return refs


def queue_photo_input(photo):
  if isinstance(photo, str):
    raw = photo.strip()
    for prefix in ('local:', 'file:', 'path:', 'localfile:'):
      if raw.startswith(prefix):
        raw = raw.split(':', 1)[1].strip()
        break
    if raw and (raw.startswith('/') or raw.startswith('./') or raw.startswith('../')):
      try:
        path = Path(raw)
        if path.exists() and path.is_file():
          return FSInputFile(path.as_posix())
      except Exception:
        pass
  return photo


def cache_queue_photo_bytes(item, raw: bytes, ext: str = '.jpg') -> str | None:
  try:
    uploads_dir = queue_photo_cache_dir()
    safe_ext = (ext or '.jpg').lower()
    if safe_ext not in {'.jpg', '.jpeg', '.png', '.webp'}:
      safe_ext = '.jpg'
    item_id = None
    if hasattr(item, 'id'):
      item_id = getattr(item, 'id')
    elif hasattr(item, 'keys') and 'id' in item.keys():
      item_id = item['id']
    safe_name = f"queue_{item_id or int(time.time()*1000)}{safe_ext}"
    target = uploads_dir / safe_name
    target.write_bytes(raw)
    photo_ref = f"local:{target.as_posix()}"
    if item_id is not None:
      try:
        qi_cols = {r['name'] for r in db.conn.execute("PRAGMA table_info(queue_items)").fetchall()}
        if 'qr_local_path' in qi_cols:
          db.conn.execute("UPDATE queue_items SET qr_file_id=?, qr_local_path=? WHERE id=?", (photo_ref, photo_ref, item_id))
        else:
          db.conn.execute("UPDATE queue_items SET qr_file_id=? WHERE id=?", (photo_ref, item_id))
        db.conn.commit()
      except Exception:
        logging.exception('failed to persist local qr cache for item %s', item_id)
    return photo_ref
  except Exception:
    logging.exception('cache_queue_photo_bytes failed')
    return None

def group_price_for_take(chat_id: int, thread_id: int | None, operator_key: str, mode: str) -> float:
  price = db.get_group_price(chat_id, thread_id, operator_key, mode)
  if price is not None:
    return float(price)
  return float(get_mode_price(operator_key, mode, None))

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


def credit_referral_bonus(source_user_id: int, earned_amount: float) -> tuple[int | None, float]:
  if float(earned_amount or 0) <= 0:
    return None, 0.0
  row = db.get_user(int(source_user_id))
  if not row or 'referred_by' not in row.keys() or not row['referred_by']:
    return None, 0.0
  referrer_id = int(row['referred_by'])
  bonus = round(float(earned_amount) * 0.05, 2)
  if bonus <= 0:
    return referrer_id, 0.0
  db.add_balance(referrer_id, bonus)
  db.conn.execute("UPDATE users SET ref_earned=COALESCE(ref_earned,0)+? WHERE user_id=?", (bonus, referrer_id))
  db.conn.commit()
  return referrer_id, bonus


def operator_command_map() -> dict[str, str]:
  mapping = {}
  for key, data in OPERATORS.items():
    cmd = str(data.get('command') or f'/{key}').strip().lower()
    if not cmd.startswith('/'):
      cmd = '/' + cmd
    mapping[cmd] = key
  return mapping


def phone_locked_until_next_msk_day(normalized_phone: str) -> bool:
  start, end = msk_day_window()
  row = db.conn.execute(
    "SELECT COUNT(*) AS c FROM queue_items WHERE normalized_phone=? AND work_started_at IS NOT NULL AND work_started_at >= ? AND work_started_at < ?",
    (normalized_phone, start, end),
  ).fetchone()
  return int((row["c"] if row else 0) or 0) >= 2


def phone_submit_limit_reached_today(normalized_phone: str) -> bool:
  start, end = msk_day_window()
  row = db.conn.execute(
    "SELECT COUNT(*) AS c FROM queue_items WHERE normalized_phone=? AND created_at >= ? AND created_at < ?",
    (normalized_phone, start, end),
  ).fetchone()
  return int((row["c"] if row else 0) or 0) >= 2


def user_today_queue_items(user_id: int):
  start, end = msk_day_window()
  return db.conn.execute(
    "SELECT * FROM queue_items WHERE user_id=? AND created_at >= ? AND created_at < ? ORDER BY id DESC",
    (user_id, start, end),
  ).fetchall()


def user_active_queue_items(user_id: int):
  return db.conn.execute(
    "SELECT * FROM queue_items WHERE user_id=? AND status IN ('queued','taken','in_progress') ORDER BY id DESC",
    (user_id,),
  ).fetchall()


def queue_position(item_id: int):
  row = db.conn.execute("SELECT operator_key, mode, status FROM queue_items WHERE id=?", (item_id,)).fetchone()
  if not row or row['status'] != 'queued':
    return None
  pos = db.conn.execute(
    "SELECT COUNT(*) AS c FROM queue_items WHERE operator_key=? AND mode=? AND status='queued' AND id <= ?",
    (row['operator_key'], row['mode'], item_id),
  ).fetchone()
  return int((pos['c'] if pos else 0) or 0)


def remove_queue_item(item_id: int, reason: str = 'removed', admin_id: int | None = None):
  db.conn.execute("UPDATE queue_items SET status='failed', fail_reason=?, completed_at=? WHERE id=? AND status='queued'", (reason, now_str(), item_id))
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
  ops_text = "\n".join([f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}" for row in ops]) or "• Данных пока нет"
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
  "mts_premium": ("5312126452043363774", "🔴"),
  "mega": ("5229218997521631084", "🟢"),
  "bil": ("5280919528908267119", "🟡"),
  "t2": ("5244453379664534900", "⚫"),
  "vtb": ("5427154326294376920", "🔵"),
  "gaz": ("5280751174780199841", "🔷"),
}

def op_emoji_html(operator_key: str) -> str:
  emoji_id, fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))
  if emoji_id:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'
  return fallback

def op_html(operator_key: str) -> str:
  return f"{op_emoji_html(operator_key)} <b>{escape(OPERATORS[operator_key]['title'])}</b>"

def op_text(operator_key: str) -> str:
  fallback = CUSTOM_OPERATOR_EMOJI.get(operator_key, ("", "📱"))[1]
  return f"{fallback} {OPERATORS[operator_key]['title']}"


def op_button_label(operator_key: str, *, with_fallback: bool = True) -> str:
  title = OPERATORS[operator_key]['title']
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


async def send_banner_message(entity, banner_path: str, caption: str, reply_markup=None):
  if Path(banner_path).exists():
    if hasattr(entity, 'answer_photo'):
      return await entity.answer_photo(FSInputFile(banner_path), caption=caption, reply_markup=reply_markup)
    return await entity.message.answer_photo(FSInputFile(banner_path), caption=caption, reply_markup=reply_markup)
  if hasattr(entity, 'answer'):
    return await entity.answer(caption, reply_markup=reply_markup)
  return await entity.message.answer(caption, reply_markup=reply_markup)


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



async def send_db_backup(bot: Bot, reason: str = "auto"):
  channel_id = backup_channel_id()
  if not channel_id:
    return False
  db_path = Path(DB_PATH)
  if not db_path.exists():
    logging.warning("DB backup skipped: DB file not found")
    return False
  backup_dir = Path("db_backups")
  backup_dir.mkdir(exist_ok=True)
  stamp = msk_now().strftime("%Y%m%d_%H%M%S")
  target = backup_dir / f"botdb_{reason}_{stamp}.db"
  try:
    target.write_bytes(db_path.read_bytes())
    caption = (
      "<b>🗄 Автовыгрузка базы данных</b>\n\n"
      f"🕒 {escape(now_str())}\n"
      f"🔖 Причина: <b>{escape(reason)}</b>"
    )
    await bot.send_document(channel_id, FSInputFile(str(target)), caption=caption)
    logging.info("DB backup sent to %s (%s)", channel_id, reason)
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
  channel_id = int(db.get_setting("log_channel_id", str(LOG_CHANNEL_ID) or "0") or 0)
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



@router.callback_query(F.data == "admin:miniapp_settings")
async def admin_miniapp_settings(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_miniapp_settings(), reply_markup=miniapp_settings_kb())
  await callback.answer()

@router.callback_query(F.data.startswith("admin:miniapp_preview:"))
async def admin_miniapp_preview(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  section = callback.data.split(':')[-1]
  path = {
    'home': '/',
    'manuals': '/manuals',
    'basics': '/manuals/basics',
  }.get(section, '/')
  url = miniapp_url(path)
  kb = InlineKeyboardBuilder()
  kb.button(text='🚀 Открыть', url=url)
  kb.button(text='↩️ Назад', callback_data='admin:miniapp_settings')
  kb.adjust(1)
  titles = {'home': 'главной', 'manuals': 'мануалов', 'basics': 'раздела Основы'}
  await callback.message.answer(f"<b>Превью {titles.get(section, 'Mini App')}</b>\n\n<code>{escape(url)}</code>", reply_markup=kb.as_markup())
  await callback.answer()

@router.callback_query(F.data.startswith("admin:miniapp_reset:"))
async def admin_miniapp_reset_single(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  section = callback.data.split(':')[-1]
  if section == 'home':
    db.set_setting('miniapp_home_title', 'DVE APP')
    db.set_setting('miniapp_home_subtitle', 'Главный центр: сдача eSIM, мануалы, профиль и быстрый доступ к разделам.')
    label = 'Главная'
  else:
    db.set_setting(manual_setting_key(section), '')
    label = manual_title(section)
  await state.clear()
  await safe_edit_or_send(callback, render_miniapp_settings(), reply_markup=miniapp_settings_kb())
  await callback.answer(f'{label} сброшен')


@router.callback_query(F.data.startswith("admin:miniapp_edit:"))
async def admin_miniapp_set_text(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  section = callback.data.split(':')[-1]
  await state.set_state(AdminStates.waiting_miniapp_text)
  await state.update_data(miniapp_section=section)
  await callback.message.answer(
    f"Отправьте новый текст для раздела <b>{manual_title(section)}</b> одним сообщением.\n\nБот применит его и в mini app, и в Telegram-мануалах."
  )
  await callback.answer()

@router.callback_query(F.data == "admin:miniapp_set_submit_bot")
async def admin_miniapp_set_submit_bot(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_miniapp_submit_bot)
  current = (db.get_setting('miniapp_submit_bot', '@DiamondVaultE_bot') or '@DiamondVaultE_bot').strip()
  await callback.message.answer(
    f"""Отправьте @username бота или полную ссылку, куда должна вести кнопка сдачи QR.

Сейчас: <code>{escape(current)}</code>"""
  )
  await callback.answer()

@router.callback_query(F.data == "admin:miniapp_reset_text")
async def admin_miniapp_reset_text(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  db.set_setting('miniapp_basics_text', '')
  db.set_setting('miniapp_mts_text', '')
  db.set_setting('miniapp_beeline_text', '')
  db.set_setting('miniapp_vtbgaz_text', '')
  db.set_setting('miniapp_home_title', 'DVE APP')
  db.set_setting('miniapp_home_subtitle', 'Главный центр: сдача eSIM, мануалы, профиль и быстрый доступ к разделам.')
  await state.clear()
  await safe_edit_or_send(callback, render_miniapp_settings(), reply_markup=miniapp_settings_kb())
  await callback.answer('Настройки Mini App сброшены')

@router.message(AdminStates.waiting_miniapp_text)
async def admin_miniapp_text_value(message: Message, state: FSMContext):
  if not is_admin(message.from_user.id):
    return
  raw = (message.html_text or message.text or '').strip()
  if not raw:
    await message.answer('Отправьте текст одним сообщением.')
    return
  data = await state.get_data()
  section = data.get('miniapp_section', 'basics')
  db.set_setting(manual_setting_key(section), raw)
  await state.clear()
  await message.answer(f'✅ Текст для раздела {manual_title(section)} обновлён.')
  await message.answer(render_miniapp_settings(), reply_markup=miniapp_settings_kb())

@router.message(AdminStates.waiting_miniapp_submit_bot)
async def admin_miniapp_submit_bot_value(message: Message, state: FSMContext):
  if not is_admin(message.from_user.id):
    return
  raw = (message.text or '').strip()
  if not raw:
    await message.answer("Отправьте @username или ссылку.")
    return
  db.set_setting('miniapp_submit_bot', raw)
  await state.clear()
  await message.answer("✅ Кнопка сдачи QR обновлена.")
  await message.answer(render_miniapp_settings(), reply_markup=miniapp_settings_kb())

@router.callback_query(F.data == "menu:manuals")
async def menu_manuals(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  await replace_banner_message(
    callback,
    db.get_setting('mini_manuals_banner_path', MINI_MANUALS_BANNER),
    telegram_manuals_menu_text(),
    telegram_manuals_menu_kb(),
  )
  await callback.answer()


@router.callback_query(F.data == "menu:manuals:basics")
async def menu_manuals_basics(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  raw = db.get_setting('miniapp_basics_text', '') or ''
  await safe_edit_or_send(callback, render_telegram_manual('basics', raw), reply_markup=telegram_manual_section_kb('basics'))
  await callback.answer()


@router.callback_query(F.data == "menu:manuals:mts")
async def menu_manuals_mts(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  raw = db.get_setting('miniapp_mts_text', '') or ''
  await safe_edit_or_send(callback, render_telegram_manual('mts', raw), reply_markup=telegram_manual_section_kb('mts'))
  await callback.answer()


@router.callback_query(F.data == "menu:manuals:beeline")
async def menu_manuals_beeline(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  raw = db.get_setting('miniapp_beeline_text', '') or ''
  await safe_edit_or_send(callback, render_telegram_manual('beeline', raw), reply_markup=telegram_manual_section_kb('beeline'))
  await callback.answer()


@router.callback_query(F.data == "menu:manuals:vtbgaz")
async def menu_manuals_vtbgaz(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  raw = db.get_setting('miniapp_vtbgaz_text', '') or ''
  await safe_edit_or_send(callback, render_telegram_manual('vtbgaz', raw), reply_markup=telegram_manual_section_kb('vtbgaz'))
  await callback.answer()


@router.message(Command("miniapp"))
async def miniapp_cmd(message: Message, state: FSMContext):
  await state.clear()
  await remove_reply_keyboard(message)
  if not await ensure_required_subscription_entity(message, message.bot, message.from_user.id):
    return
  await send_banner_message(message, db.get_setting('profile_banner_path', PROFILE_BANNER), '<b>✨ Mini App</b>\n\nВстроенное меню Diamond Vault Esim открывается отдельным окном внутри Telegram.', miniapp_home_kb())


@router.callback_query(F.data == "menu:miniapp")
async def menu_miniapp(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  await replace_banner_message(callback, db.get_setting('profile_banner_path', PROFILE_BANNER), '<b>✨ Mini App</b>\n\nВстроенное меню Diamond Vault Esim открывается отдельным окном внутри Telegram.', miniapp_home_kb())
  await callback.answer()


@router.callback_query(F.data == "miniapp:help")
async def miniapp_help(callback: CallbackQuery):
  await callback.answer()
  await callback.message.answer(miniapp_help_text())


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
  await callback.answer()

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
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
    return
  if is_user_blocked(callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
  else:
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
  await callback.answer()


@router.callback_query(F.data == "menu:mirror")
async def mirror_menu(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  await replace_banner_message(
    callback,
    db.get_setting('start_banner_path', START_BANNER),
    render_mirror_menu(callback.from_user.id),
    mirror_menu_kb(),
  )
  await callback.answer()

@router.callback_query(F.data == "mirror:list")
async def mirror_list(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  await replace_banner_message(
    callback,
    db.get_setting('start_banner_path', START_BANNER),
    render_mirror_menu(callback.from_user.id),
    mirror_menu_kb(),
  )
  await callback.answer()

@router.callback_query(F.data == "mirror:create")
async def mirror_create(callback: CallbackQuery, state: FSMContext):
  await state.set_state(MirrorStates.waiting_token)
  kb = InlineKeyboardBuilder()
  kb.button(text="↩️ Назад", callback_data="menu:mirror")
  kb.adjust(1)
  await replace_banner_message(
    callback,
    db.get_setting('start_banner_path', START_BANNER),
    "<b>🔗 Создание зеркала</b>\n\n"
    "Отправьте <b>API token</b> нового бота от <b>@BotFather</b>.\n"
    "Этот бот будет сохранён как зеркало сервиса без выдачи дополнительных прав.",
    kb.as_markup(),
  )
  await callback.answer()

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
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
    return
  items = user_active_queue_items(callback.from_user.id)
  await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id), my_numbers_kb(items))
  await callback.answer()

@router.callback_query(F.data == "menu:profile")
async def menu_profile(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
    return
  await replace_banner_message(callback, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
  await callback.answer()

@router.callback_query(F.data == "menu:ref")
async def menu_ref(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
    return
  await replace_banner_message(callback, db.get_setting('profile_banner_path', PROFILE_BANNER), render_referral(callback.from_user.id), referral_kb(callback.from_user.id))
  await callback.answer()

@router.callback_query(F.data == "menu:withdraw")
async def menu_withdraw(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
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
  await callback.answer()

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
  await callback.answer()

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
    kb.button(text=op_text(key), callback_data=f"takeop:{key}")
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
  caption = queue_caption(item) + "\n\n👇 Выберите нужное действие:"
  if getattr(callback.message, 'photo', None):
    await callback.message.answer_photo(queue_photo_input(item['qr_file_id']), caption=caption, reply_markup=admin_queue_kb(item))
  else:
    await callback.message.answer_photo(queue_photo_input(item['qr_file_id']), caption=caption, reply_markup=admin_queue_kb(item))
  await callback.answer()


@router.callback_query(F.data == "menu:submit")
async def submit_start_cb(callback: CallbackQuery, state: FSMContext):
  if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
    return
  if is_user_blocked(callback.from_user.id):
    await callback.answer("Аккаунт заблокирован", show_alert=True)
    return
  if not is_numbers_enabled():
    await callback.answer("Сдача номеров выключена", show_alert=True)
    return
  await state.set_state(SubmitStates.waiting_mode)
  await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b> ESIM Diamond Vault </b>\n\n<b>📲 Сдать eSIM - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
  await callback.answer()


@router.callback_query(F.data == "mode:back")
async def mode_back(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  if is_user_blocked(callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
  else:
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
  await callback.answer()

@router.callback_query(F.data.startswith("mode:"))
async def choose_mode(callback: CallbackQuery, state: FSMContext):
  mode = callback.data.split(":", 1)[1]
  if mode not in {"hold", "no_hold"}:
    await callback.answer()
    return
  await state.update_data(mode=mode)
  await state.set_state(SubmitStates.waiting_operator)
  mode_title = "⏳ Холд" if mode == "hold" else "⚡ Безхолд"
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
  await callback.answer()


@router.callback_query(F.data == "op:back")
async def op_back(callback: CallbackQuery, state: FSMContext):
  await state.set_state(SubmitStates.waiting_mode)
  await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b> ESIM Diamond Vault </b>\n\n<b>📲 Сдать eSIM - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
  await callback.answer()


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
    "<b> ESIM Diamond Vault </b>\n\n<b>📨 Отправьте QR-код - Фото сообщением</b>\n\n👉 <b>Требуется:</b>\n▫️ Фото QR\n▫️ В подписи укажите номер\n\n🔰 <b>Допустимый формат номера:</b>\n<blockquote>+79991234567 «+7»\n79991234567  «7»\n89991234567  «8»</blockquote>\n\nЕсли передумали нажмите ниже - Отмена",
    cancel_inline_kb("op:back"),
  )
  await callback.answer()


@router.message(WithdrawStates.waiting_amount, F.text == "↩️ Назад")
@router.message(WithdrawStates.waiting_payment_link, F.text == "↩️ Назад")
async def global_back(message: Message, state: FSMContext):
  await state.clear()
  await send_banner_message(message, db.get_setting('start_banner_path', START_BANNER), render_start(message.from_user.id), main_menu())


async def persist_message_qr_photo(message: Message) -> str:
  photo = message.photo[-1]
  file_id = photo.file_id
  try:
    telegram_file = await message.bot.get_file(file_id)
    file_bytes = io.BytesIO()
    await message.bot.download_file(telegram_file.file_path, destination=file_bytes)
    file_bytes.seek(0)
    ext = Path(getattr(telegram_file, 'file_path', '')).suffix.lower() or '.jpg'
    uploads_dir = queue_photo_cache_dir()
    safe_name = f"qr_{message.from_user.id}_{int(time.time()*1000)}{ext}"
    target = uploads_dir / safe_name
    raw = file_bytes.read()
    target.write_bytes(raw)
    logging.info('qr photo cached locally: %s', target.as_posix())
    return f"local:{target.as_posix()}"
  except Exception:
    logging.exception('persist_message_qr_photo failed; fallback to file_id')
    return file_id

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
  if phone_locked_until_next_msk_day(phone):
    await message.answer("<b>⛔ Этот номер уже вставал сегодня.</b>\n\nПовторная сдача будет доступна после <b>00:00 МСК следующего дня</b>.", reply_markup=cancel_inline_kb())
    return
  photo_ref = await persist_message_qr_photo(message)
  item_id = create_queue_item_ext(
    message.from_user.id,
    message.from_user.username or "",
    message.from_user.full_name,
    operator_key,
    phone,
    photo_ref,
    mode,
    getattr(message.bot, "token", BOT_TOKEN),
  )
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


@router.message(F.text == "🏦 Вывод средств")
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
      "<b>🏦 Запросить выплату</b>\n\n"
      f"📉 Минимальный вывод: <b>{usd(minimum)}</b>\n"
      f"💰 Ваш баланс: <b>{usd(balance)}</b>\n\n"
      "⚠️ Укажите сумму числом. Пример: <code>12.5</code>",
      reply_markup=cancel_inline_kb("menu:profile"),
    )
    return
  minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
  user = db.get_user(message.from_user.id)
  balance = float(user["balance"] if user else 0)
  if amount < minimum:
    await message.answer(f"⚠️ <b>Сумма ниже минимального порога.</b> Минимум: <b>{usd(minimum)}</b>", reply_markup=cancel_inline_kb("menu:profile"))
    return
  if amount > balance:
    await message.answer("⚠️ <b>На балансе недостаточно средств.</b>", reply_markup=cancel_inline_kb("menu:profile"))
    return
  await state.clear()
  await message.answer(
    "<b>Подтверждение вывода</b>\n\n"
    f"🗓 Дата: <b>{now_str()}</b>\n"
    f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
    "Подтвердить отправку заявки?",
    reply_markup=confirm_withdraw_kb(amount),
  )


@router.callback_query(F.data == "withdraw_cancel")
async def withdraw_cancel(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  await callback.message.edit_text("❌ Запрос на выплату отменён.")
  await send_banner_message(callback.message, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
  await callback.answer()


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
    "✅ Запрос на вывод принят. Она отправлена в канал выплат." if sent_ok else "⚠️ Заявка создана, но сообщение в канал выплат не отправилось. Проверь логи и настройки канала."
  )
  await send_banner_message(callback.message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
  await callback.answer()



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
  await callback.answer()


@router.callback_query(F.data == "admin:summary")
async def admin_summary(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_admin_summary(), reply_markup=admin_summary_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:summary_by_date")
async def admin_summary_by_date(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_summary_date)
  await callback.message.answer("📅 Введите дату в формате <code>ДД-ММ-ГГГГ</code> или <code>ДД.ММ.ГГГГ</code>.")
  await callback.answer()


@router.callback_query(F.data == "admin:treasury")
async def admin_treasury(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_admin_treasury(), reply_markup=treasury_kb())
  await callback.answer()



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
  await callback.answer()

@router.callback_query(F.data == "admin:withdraws")
async def admin_withdraws(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_admin_withdraws(), reply_markup=admin_back_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:hold")
async def admin_hold(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_admin_hold(), reply_markup=hold_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:prices")
async def admin_prices(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_admin_prices(), reply_markup=prices_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:roles")
async def admin_roles(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_roles(), reply_markup=roles_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:workspaces")
async def admin_workspaces(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_workspaces(), reply_markup=workspaces_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:group_stats_panel")
async def admin_group_stats_panel(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, "<b>📈 Выберите группу / топик для статистики:</b>", reply_markup=group_stats_list_kb())
  await callback.answer()

@router.callback_query(F.data.startswith("admin:groupstat:"))
async def admin_groupstat_open(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  _, _, chat_id, thread_id = callback.data.split(":")
  chat_id = int(chat_id)
  thread = int(thread_id)
  thread = None if thread == 0 else thread
  await safe_edit_or_send(callback, render_single_group_stats(chat_id, thread), reply_markup=single_group_stats_kb(chat_id, thread))
  await callback.answer()

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
  await callback.answer()



@router.callback_query(F.data == "admin:operator_modes")
async def admin_operator_modes(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
  await callback.answer()

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
  await callback.answer()


@router.callback_query(F.data == "admin:templates")
async def admin_templates(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_templates(), reply_markup=design_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_broadcast(), reply_markup=broadcast_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:broadcast_write")
async def admin_broadcast_write(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_broadcast_text)
  await callback.message.answer(
    "Отправьте текст рассылки одним сообщением.\n\nМожно использовать HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
  )
  await callback.answer()


@router.callback_query(F.data == "admin:broadcast_preview")
async def admin_broadcast_preview(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  ad = db.get_setting("broadcast_text", "").strip()
  await callback.message.answer(ad or "Рассылка пока пустая.")
  await callback.answer()


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
  await callback.answer()


@router.callback_query(F.data == "admin:usernames")
async def admin_usernames(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  content = db.export_usernames().encode("utf-8")
  file = BufferedInputFile(content, filename="usernames.txt")
  await callback.message.answer_document(file, caption="📥 Собранные username и user_id")
  await callback.answer()


@router.callback_query(F.data == "admin:download_db")
async def admin_download_db(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  path = Path(DB_PATH)
  if not path.exists():
    await callback.answer("База не найдена", show_alert=True)
    return
  await callback.message.answer_document(FSInputFile(path), caption="<b>📦 SQLite база</b>")
  await callback.answer()

@router.callback_query(F.data == "admin:upload_db")
async def admin_upload_db(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_db_upload)
  await callback.message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")
  await callback.answer()


@router.callback_query(F.data == "admin:set_start_text")
async def admin_set_start_text(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_start_text)
  await callback.message.answer(
    "Отправьте новый стартовый текст в формате:\n\n<code>Заголовок\nПодзаголовок\nОписание</code>\n\nПервые 2 строки пойдут в шапку, остальное в описание."
  )
  await callback.answer()


@router.callback_query(F.data == "admin:set_ad_text")
async def admin_set_ad_text(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_ad_text)
  await callback.message.answer(
    "Отправьте текст рассылки.\n\nМожно писать красивыми шаблонами и использовать HTML Telegram."
  )
  await callback.answer()


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
  await callback.answer()


@router.callback_query(F.data.startswith("admin:pick_operator_emoji:"))
async def admin_pick_operator_emoji(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  key = callback.data.split(":", 2)[-1].strip().lower()
  if key not in OPERATORS:
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
  await callback.answer()


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
  await callback.answer()

@router.callback_query(F.data == "admin:remove_operator")
async def admin_remove_operator(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_remove_operator)
  removable = []
  base_keys = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
  for key, data in OPERATORS.items():
    if key not in base_keys:
      removable.append(f"• <code>{key}</code> — {escape(data.get('title', key))}")
  removable_text = "\n".join(removable) if removable else "• Нет добавленных операторов для удаления."
  await callback.message.answer(
    "<b>➖ Удаление оператора</b>\n\n"
    "Отправьте <code>key</code> оператора, которого нужно удалить.\n\n"
    f"{removable_text}\n\n"
    "Базовых операторов удалить нельзя."
  )
  await callback.answer()

@router.callback_query(F.data == "admin:set_hold")
async def admin_set_hold(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_hold)
  await callback.message.answer("Введите новый Холд в минутах:")
  await callback.answer()


@router.callback_query(F.data == "admin:set_min_withdraw")
async def admin_set_min_withdraw(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_min_withdraw)
  await callback.message.answer("Введите новый минимальный вывод в $:")
  await callback.answer()


@router.callback_query(F.data == "admin:treasury_add")
async def admin_treasury_add(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_treasury_invoice)
  await callback.message.answer("Введите сумму пополнения казны в $ для создания <b>Crypto Bot invoice</b>:")
  await callback.answer()


@router.callback_query(F.data == "admin:treasury_sub")
async def admin_treasury_sub(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_treasury_sub)
  await callback.message.answer("Введите сумму вывода казны в $ — будет создан <b>реальный чек Crypto Bot</b>:")
  await callback.answer()


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
  await callback.answer()


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
  await callback.answer()


@router.callback_query(F.data == "admin:ws_help_group")
async def admin_ws_help_group(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.answer("Чтобы добавить рабочую группу, зайдите в нужную группу и отправьте команду <code>/work</code>.")
  await callback.answer()


@router.callback_query(F.data == "admin:ws_help_topic")
async def admin_ws_help_topic(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.answer("Чтобы добавить рабочий топик, зайдите в нужный топик и отправьте команду <code>/topic</code>.")
  await callback.answer()


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
  base_keys = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
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

  db.set_setting('extra_operators_json', json.dumps(extra_items, ensure_ascii=False))
  db.set_setting(f'price_{key}', str(price))
  db.set_setting(f'price_hold_{key}', str(price))
  db.set_setting(f'price_no_hold_{key}', str(price))
  db.set_setting(f'allow_hold_{key}', db.get_setting(f'allow_hold_{key}', '1'))
  db.set_setting(f'allow_no_hold_{key}', db.get_setting(f'allow_no_hold_{key}', '1'))
  CUSTOM_OPERATOR_EMOJI[key] = (emoji_id, fallback_emoji)
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
  base_keys = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
  if key in base_keys:
    await message.answer("Базового оператора удалить нельзя.")
    return
  if key not in OPERATORS:
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
  db.conn.execute("DELETE FROM settings WHERE key IN (?,?,?,?,?)", (f'price_{key}', f'price_hold_{key}', f'price_no_hold_{key}', f'allow_hold_{key}', f'allow_no_hold_{key}'))
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
    "<b>✅ Заявка на вывод из казны создана</b>\n\n"
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


@router.message(Command("mts", "mtc", "mtspremium", "mtssalon", "bil", "mega", "t2"))
async def legacy_take_commands(message: Message):
  if not is_operator_or_admin(message.from_user.id):
    return
  await message.answer("Команды операторов отключены. Используй <b>/esim</b>.")


@router.message(F.text.regexp(r"^/[A-Za-z0-9_]+(?:@\w+)?$"))
async def dynamic_operator_command_stub(message: Message):
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
    await message.answer("Использовать /esim могут только операторы, админы бота или админы этой группы.")
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
  await callback.answer()

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
    kb.button(text=op_text(key), callback_data=f"takeop:{key}")
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
  caption = queue_caption(item) + "\n\n👇 Выберите нужное действие:"
  if getattr(callback.message, 'photo', None):
    await callback.message.answer_photo(queue_photo_input(item['qr_file_id']), caption=caption, reply_markup=admin_queue_kb(item))
  else:
    await callback.message.answer_photo(queue_photo_input(item['qr_file_id']), caption=caption, reply_markup=admin_queue_kb(item))
  await callback.answer()


@router.callback_query(F.data == "menu:submit")
async def submit_start_cb(callback: CallbackQuery, state: FSMContext):
  if not await is_user_joined_required_group(callback.bot, callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), '<b>🔒 Доступ ограничен</b>\n\nДля доступа к функционалу нужна обязательная подписка на указанную группу.\n\nПосле вступления нажмите <b>«Проверить подписку»</b>, чтобы продолжить работу.', required_join_kb().as_markup())
    await callback.answer()
    return
  if is_user_blocked(callback.from_user.id):
    await callback.answer("Аккаунт заблокирован", show_alert=True)
    return
  if not is_numbers_enabled():
    await callback.answer("Сдача номеров выключена", show_alert=True)
    return
  await state.set_state(SubmitStates.waiting_mode)
  await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b> ESIM Diamond Vault </b>\n\n<b>📲 Сдать eSIM - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
  await callback.answer()


@router.callback_query(F.data == "mode:back")
async def mode_back(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  if is_user_blocked(callback.from_user.id):
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), blocked_text(), None)
  else:
    await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), render_start(callback.from_user.id), main_menu())
  await callback.answer()

@router.callback_query(F.data.startswith("mode:"))
async def choose_mode(callback: CallbackQuery, state: FSMContext):
  mode = callback.data.split(":", 1)[1]
  if mode not in {"hold", "no_hold"}:
    await callback.answer()
    return
  await state.update_data(mode=mode)
  await state.set_state(SubmitStates.waiting_operator)
  mode_title = "⏳ Холд" if mode == "hold" else "⚡ Безхолд"
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
  await callback.answer()


@router.callback_query(F.data == "op:back")
async def op_back(callback: CallbackQuery, state: FSMContext):
  await state.set_state(SubmitStates.waiting_mode)
  await replace_banner_message(callback, db.get_setting('start_banner_path', START_BANNER), "<b> ESIM Diamond Vault </b>\n\n<b>📲 Сдать eSIM - ЕСИМ</b>\n\nСначала выберите режим работы для новой заявки:", mode_kb())
  await callback.answer()


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
    "<b> ESIM Diamond Vault </b>\n\n<b>📨 Отправьте QR-код - Фото сообщением</b>\n\n👉 <b>Требуется:</b>\n▫️ Фото QR\n▫️ В подписи укажите номер\n\n🔰 <b>Допустимый формат номера:</b>\n<blockquote>+79991234567 «+7»\n79991234567  «7»\n89991234567  «8»</blockquote>\n\nЕсли передумали нажмите ниже - Отмена",
    cancel_inline_kb("op:back"),
  )
  await callback.answer()


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
  if phone_locked_until_next_msk_day(phone):
    await message.answer("<b>⛔ Этот номер уже вставал сегодня.</b>\n\nПовторная сдача будет доступна после <b>00:00 МСК следующего дня</b>.", reply_markup=cancel_inline_kb())
    return
  photo_ref = await persist_message_qr_photo(message)
  item_id = create_queue_item_ext(
    message.from_user.id,
    message.from_user.username or "",
    message.from_user.full_name,
    operator_key,
    phone,
    photo_ref,
    mode,
    getattr(message.bot, "token", BOT_TOKEN),
  )
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


@router.message(F.text == "🏦 Вывод средств")
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
      "<b>🏦 Запросить выплату</b>\n\n"
      f"📉 Минимальный вывод: <b>{usd(minimum)}</b>\n"
      f"💰 Ваш баланс: <b>{usd(balance)}</b>\n\n"
      "⚠️ Укажите сумму числом. Пример: <code>12.5</code>",
      reply_markup=cancel_inline_kb("menu:profile"),
    )
    return
  minimum = float(db.get_setting("min_withdraw", str(MIN_WITHDRAW)))
  user = db.get_user(message.from_user.id)
  balance = float(user["balance"] if user else 0)
  if amount < minimum:
    await message.answer(f"⚠️ <b>Сумма ниже минимального порога.</b> Минимум: <b>{usd(minimum)}</b>", reply_markup=cancel_inline_kb("menu:profile"))
    return
  if amount > balance:
    await message.answer("⚠️ <b>На балансе недостаточно средств.</b>", reply_markup=cancel_inline_kb("menu:profile"))
    return
  await state.clear()
  await message.answer(
    "<b>Подтверждение вывода</b>\n\n"
    f"🗓 Дата: <b>{now_str()}</b>\n"
    f"💸 Сумма: <b>{usd(amount)}</b>\n\n"
    "Подтвердить отправку заявки?",
    reply_markup=confirm_withdraw_kb(amount),
  )


@router.callback_query(F.data == "withdraw_cancel")
async def withdraw_cancel(callback: CallbackQuery, state: FSMContext):
  await state.clear()
  await callback.message.edit_text("❌ Запрос на выплату отменён.")
  await send_banner_message(callback.message, db.get_setting('profile_banner_path', PROFILE_BANNER), render_profile(callback.from_user.id), profile_kb())
  await callback.answer()


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
    "✅ Запрос на вывод принят. Она отправлена в канал выплат." if sent_ok else "⚠️ Заявка создана, но сообщение в канал выплат не отправилось. Проверь логи и настройки канала."
  )
  await send_banner_message(callback.message, db.get_setting('withdraw_banner_path', WITHDRAW_BANNER), render_withdraw(callback.from_user.id), cancel_inline_kb("menu:profile"))
  await callback.answer()



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
  await callback.answer()


@router.callback_query(F.data == "admin:summary")
async def admin_summary(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_admin_summary(), reply_markup=admin_summary_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:summary_by_date")
async def admin_summary_by_date(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_summary_date)
  await callback.message.answer("📅 Введите дату в формате <code>ДД-ММ-ГГГГ</code> или <code>ДД.ММ.ГГГГ</code>.")
  await callback.answer()


@router.callback_query(F.data == "admin:treasury")
async def admin_treasury(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_admin_treasury(), reply_markup=treasury_kb())
  await callback.answer()



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
  await callback.answer()

@router.callback_query(F.data == "admin:withdraws")
async def admin_withdraws(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_admin_withdraws(), reply_markup=admin_back_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:hold")
async def admin_hold(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_admin_hold(), reply_markup=hold_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:prices")
async def admin_prices(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_admin_prices(), reply_markup=prices_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:roles")
async def admin_roles(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_roles(), reply_markup=roles_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:workspaces")
async def admin_workspaces(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_workspaces(), reply_markup=workspaces_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:group_stats_panel")
async def admin_group_stats_panel(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, "<b>📈 Выберите группу / топик для статистики:</b>", reply_markup=group_stats_list_kb())
  await callback.answer()

@router.callback_query(F.data.startswith("admin:groupstat:"))
async def admin_groupstat_open(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  _, _, chat_id, thread_id = callback.data.split(":")
  chat_id = int(chat_id)
  thread = int(thread_id)
  thread = None if thread == 0 else thread
  await safe_edit_or_send(callback, render_single_group_stats(chat_id, thread), reply_markup=single_group_stats_kb(chat_id, thread))
  await callback.answer()

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
  await callback.answer()



@router.callback_query(F.data == "admin:operator_modes")
async def admin_operator_modes(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_operator_modes(), reply_markup=operator_modes_kb())
  await callback.answer()

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
  await callback.answer()


@router.callback_query(F.data == "admin:templates")
async def admin_templates(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_templates(), reply_markup=design_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.edit_text(render_broadcast(), reply_markup=broadcast_kb())
  await callback.answer()


@router.callback_query(F.data == "admin:broadcast_write")
async def admin_broadcast_write(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_broadcast_text)
  await callback.message.answer(
    "Отправьте текст рассылки одним сообщением.\n\nМожно использовать HTML Telegram: <code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, <code>&lt;blockquote&gt;</code>."
  )
  await callback.answer()


@router.callback_query(F.data == "admin:broadcast_preview")
async def admin_broadcast_preview(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  ad = db.get_setting("broadcast_text", "").strip()
  await callback.message.answer(ad or "Рассылка пока пустая.")
  await callback.answer()


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
  await callback.answer()


@router.callback_query(F.data == "admin:usernames")
async def admin_usernames(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  content = db.export_usernames().encode("utf-8")
  file = BufferedInputFile(content, filename="usernames.txt")
  await callback.message.answer_document(file, caption="📥 Собранные username и user_id")
  await callback.answer()


@router.callback_query(F.data == "admin:download_db")
async def admin_download_db(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  path = Path(DB_PATH)
  if not path.exists():
    await callback.answer("База не найдена", show_alert=True)
    return
  await callback.message.answer_document(FSInputFile(path), caption="<b>📦 SQLite база</b>")
  await callback.answer()

@router.callback_query(F.data == "admin:upload_db")
async def admin_upload_db(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_db_upload)
  await callback.message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")
  await callback.answer()


@router.callback_query(F.data == "admin:set_start_text")
async def admin_set_start_text(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_start_text)
  await callback.message.answer(
    "Отправьте новый стартовый текст в формате:\n\n<code>Заголовок\nПодзаголовок\nОписание</code>\n\nПервые 2 строки пойдут в шапку, остальное в описание."
  )
  await callback.answer()


@router.callback_query(F.data == "admin:set_ad_text")
async def admin_set_ad_text(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_ad_text)
  await callback.message.answer(
    "Отправьте текст рассылки.\n\nМожно писать красивыми шаблонами и использовать HTML Telegram."
  )
  await callback.answer()


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
  await callback.answer()

@router.callback_query(F.data == "admin:remove_operator")
async def admin_remove_operator(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_remove_operator)
  removable = []
  base_keys = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
  for key, data in OPERATORS.items():
    if key not in base_keys:
      removable.append(f"• <code>{key}</code> — {escape(data.get('title', key))}")
  removable_text = "\n".join(removable) if removable else "• Нет добавленных операторов для удаления."
  await callback.message.answer(
    "<b>➖ Удаление оператора</b>\n\n"
    "Отправьте <code>key</code> оператора, которого нужно удалить.\n\n"
    f"{removable_text}\n\n"
    "Базовых операторов удалить нельзя."
  )
  await callback.answer()

@router.callback_query(F.data == "admin:set_hold")
async def admin_set_hold(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_hold)
  await callback.message.answer("Введите новый Холд в минутах:")
  await callback.answer()


@router.callback_query(F.data == "admin:set_min_withdraw")
async def admin_set_min_withdraw(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_min_withdraw)
  await callback.message.answer("Введите новый минимальный вывод в $:")
  await callback.answer()


@router.callback_query(F.data == "admin:treasury_add")
async def admin_treasury_add(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_treasury_invoice)
  await callback.message.answer("Введите сумму пополнения казны в $ для создания <b>Crypto Bot invoice</b>:")
  await callback.answer()


@router.callback_query(F.data == "admin:treasury_sub")
async def admin_treasury_sub(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  await state.set_state(AdminStates.waiting_treasury_sub)
  await callback.message.answer("Введите сумму вывода казны в $ — будет создан <b>реальный чек Crypto Bot</b>:")
  await callback.answer()


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
  await callback.answer()


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
  await callback.answer()


@router.callback_query(F.data == "admin:ws_help_group")
async def admin_ws_help_group(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.answer("Чтобы добавить рабочую группу, зайдите в нужную группу и отправьте команду <code>/work</code>.")
  await callback.answer()


@router.callback_query(F.data == "admin:ws_help_topic")
async def admin_ws_help_topic(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await callback.message.answer("Чтобы добавить рабочий топик, зайдите в нужный топик и отправьте команду <code>/topic</code>.")
  await callback.answer()


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
  base_keys = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
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

  db.set_setting('extra_operators_json', json.dumps(extra_items, ensure_ascii=False))
  db.set_setting(f'price_{key}', str(price))
  db.set_setting(f'price_hold_{key}', str(price))
  db.set_setting(f'price_no_hold_{key}', str(price))
  db.set_setting(f'allow_hold_{key}', db.get_setting(f'allow_hold_{key}', '1'))
  db.set_setting(f'allow_no_hold_{key}', db.get_setting(f'allow_no_hold_{key}', '1'))
  CUSTOM_OPERATOR_EMOJI[key] = (emoji_id, fallback_emoji)
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
  base_keys = {'mts','mts_premium','bil','mega','t2','vtb','gaz'}
  if key in base_keys:
    await message.answer("Базового оператора удалить нельзя.")
    return
  if key not in OPERATORS:
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
  db.conn.execute("DELETE FROM settings WHERE key IN (?,?,?,?,?)", (f'price_{key}', f'price_hold_{key}', f'price_no_hold_{key}', f'allow_hold_{key}', f'allow_no_hold_{key}'))
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
    "<b>✅ Заявка на вывод из казны создана</b>\n\n"
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


@router.message(Command("mts", "mtc", "mtspremium", "mtssalon", "bil", "mega", "t2"))
async def legacy_take_commands(message: Message):
  if not is_operator_or_admin(message.from_user.id):
    return
  await message.answer("Команды операторов отключены. Используй <b>/esim</b>.")


@router.message(F.text.regexp(r"^/[A-Za-z0-9_]+(?:@\w+)?$"))
async def dynamic_operator_command_stub(message: Message):
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
    await message.answer("Использовать /esim могут только операторы, админы бота или админы этой группы.")
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
    await callback.answer()
    return
  if not is_operator_or_admin(callback.from_user.id):
    return
  text = "<b>📥 Выбор номера ESIM</b>\n\nСначала выберите режим, который нужен:"
  await safe_edit_or_send(callback, text, reply_markup=esim_mode_kb(callback.from_user.id))
  await callback.answer()


@router.callback_query(F.data.startswith("esim_mode:"))
async def esim_choose_mode(callback: CallbackQuery):
  logging.info("esim_choose_mode callback=%s", callback.data)
  if not consume_event_once("cb_esim_mode", callback.id):
    await callback.answer()
    return
  if not is_operator_or_admin(callback.from_user.id):
    return
  mode = callback.data.split(':', 1)[1]
  text = f"<b>📥 Выбор номера ESIM</b>\n\nВыбран режим: <b>{mode_label(mode)}</b>\n👇 Теперь выберите оператора:\n<i>Цена указана прямо в кнопках.</i>"
  thread_id = getattr(callback.message, 'message_thread_id', None)
  await safe_edit_or_send(callback, text, reply_markup=operators_group_kb(callback.message.chat.id, thread_id, mode, 'esim_take', 'esim:back_mode'))
  await callback.answer()


@router.callback_query(F.data.startswith("esim_take:"))
async def esim_take(callback: CallbackQuery):
  logging.info("esim_take callback=%s", callback.data)
  if not consume_event_once("cb_esim_take", callback.id):
    await callback.answer()
    return
  if not is_operator_or_admin(callback.from_user.id):
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
          referrer_id, ref_bonus = credit_referral_bonus(item.user_id, float(item.price))
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


def recent_qr_items(limit: int = 50, operator_key: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  params = []
  clauses = []
  if operator_key != "all":
    clauses.append("operator_key=?")
    params.append(operator_key)
  if status_filter == "queued":
    clauses.append("status='queued'")
  if mode_filter in {"hold", "no_hold"}:
    clauses.append("mode=?")
    params.append(mode_filter)
  if day_filter == "today":
    clauses.append("created_at>=?")
    params.append(_start_of_today_str())
  where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
  return db.conn.execute(f"SELECT * FROM queue_items {where} ORDER BY id DESC LIMIT ?", tuple(params + [limit])).fetchall()


def qr_page_items(page: int = 0, per_page: int = 1, operator_key: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  page = max(0, int(page))
  params = []
  clauses = []
  if operator_key != "all":
    clauses.append("operator_key=?")
    params.append(operator_key)
  if status_filter == "queued":
    clauses.append("status='queued'")
  if mode_filter in {"hold", "no_hold"}:
    clauses.append("mode=?")
    params.append(mode_filter)
  if day_filter == "today":
    clauses.append("created_at>=?")
    params.append(_start_of_today_str())
  where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
  total = db.conn.execute(f"SELECT COUNT(*) AS c FROM queue_items {where}", tuple(params)).fetchone()['c'] or 0
  offset = page * per_page
  rows = db.conn.execute(
    f"SELECT * FROM queue_items {where} ORDER BY id DESC LIMIT ? OFFSET ?",
    tuple(params + [per_page, offset])
  ).fetchall()
  return rows, int(total)


def admin_qr_browser_kb(index: int, total: int, operator_key: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  kb = InlineKeyboardBuilder()
  admin_operator_filter_buttons(kb, "admin:qr_filter", operator_key, index, status_filter, day_filter, mode_filter)
  admin_scope_filter_buttons(kb, "admin:qr_filter", operator_key, status_filter, day_filter, mode_filter)
  admin_mode_filter_buttons(kb, "admin:qr_filter", operator_key, status_filter, day_filter, mode_filter)
  prev_i = index - 1 if index > 0 else total - 1
  next_i = index + 1 if index < total - 1 else 0
  kb.button(text="⬅️", callback_data=f"admin:qr_numbers:{prev_i}:{operator_key}:{status_filter}:{day_filter}:{mode_filter}")
  kb.button(text=f"Стр. {index + 1}/{total}", callback_data="admin:qr_numbers:noop")
  kb.button(text="➡️", callback_data=f"admin:qr_numbers:{next_i}:{operator_key}:{status_filter}:{day_filter}:{mode_filter}")
  kb.button(text="🔢 Страница", callback_data=f"admin:qr_jump:{index}:{operator_key}:{status_filter}:{day_filter}:{mode_filter}")
  kb.button(text="🔄 Обновить", callback_data=f"admin:qr_numbers:{index}:{operator_key}:{status_filter}:{day_filter}:{mode_filter}")
  kb.button(text="📦 К очереди", callback_data=f"admin:queues:0:{operator_key}:{day_filter}:{mode_filter}")
  kb.button(text="↩️ Назад", callback_data="admin:home")
  kb.adjust(3, 2, 3, 3, 3, 1)
  return kb.as_markup()


def render_qr_browser_caption(item) -> str:
  username = item['username'] or 'без username'
  if username and username != 'без username' and not str(username).startswith('@'):
    username = '@' + str(username)
  name = item['full_name'] or 'Без имени'
  phone = pretty_phone(item['normalized_phone'])
  operator = op_text(item['operator_key'])
  mode = mode_label(item['mode'])
  status = status_label_from_row(item)
  created = item['created_at']
  user_line = (
    f"<b>👤 Человек:</b> {escape(name)}\n"
    f"<b>🆔 ID:</b> <code>{item['user_id']}</code>\n"
    f"<b>🔗 Username:</b> {escape(username)}"
  )
  return (
    f"<b>🖼 QR и номера</b>\n\n"
    f"<b>📌 Заявка:</b> <code>#{item['id']}</code>\n"
    f"<b>📱 Оператор:</b> {escape(operator)}\n"
    f"<b>🧾 Режим:</b> {escape(mode)}\n"
    f"<b>☎️ Номер:</b> <code>{phone}</code>\n"
    f"<b>📍 Статус:</b> {escape(status)}\n"
    f"<b>🕒 Создано:</b> {escape(created)}\n\n"
    + user_line
  )

async def open_qr_browser_message(message: Message, index: int = 0, operator_key: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  rows, total = qr_page_items(index, 1, operator_key, status_filter, day_filter, mode_filter)
  if not rows:
    await message.answer("<b>🖼 QR и номера</b>\n\n<i>Заявок по выбранным фильтрам пока нет.</i>", reply_markup=admin_back_kb())
    return
  idx = max(0, min(index, total - 1))
  item = rows[0]
  caption = render_qr_browser_caption(item)
  try:
    await send_queue_item_photo_to_chat(message.bot, message.chat.id, item, caption=caption, reply_markup=admin_qr_browser_kb(idx, total, operator_key, status_filter, day_filter, mode_filter), message_thread_id=getattr(message, 'message_thread_id', None))
  except Exception:
    logging.exception('admin qr browser photo send failed for item_id=%s qr=%r', item['id'] if hasattr(item, 'keys') else getattr(item, 'id', None), item['qr_file_id'] if hasattr(item, 'keys') else getattr(item, 'qr_file_id', None))
    await message.answer(caption + "\n\n<i>QR preview недоступен.</i>", reply_markup=admin_qr_browser_kb(idx, total, operator_key, status_filter, day_filter, mode_filter))

async def update_qr_browser_message(callback: CallbackQuery, index: int = 0, operator_key: str = "all", status_filter: str = "all", day_filter: str = "all", mode_filter: str = "all"):
  rows, total = qr_page_items(index, 1, operator_key, status_filter, day_filter, mode_filter)
  if not rows:
    await safe_edit_or_send(callback, "<b>🖼 QR и номера</b>\n\n<i>Заявок по выбранным фильтрам пока нет.</i>", reply_markup=admin_back_kb())
    return
  idx = max(0, min(index, total - 1))
  item = rows[0]
  caption = render_qr_browser_caption(item)
  try:
    if callback.message:
      try:
        await callback.message.delete()
      except Exception:
        pass
    await send_queue_item_photo_to_chat(callback.bot, callback.message.chat.id, item, caption=caption, reply_markup=admin_qr_browser_kb(idx, total, operator_key, status_filter, day_filter, mode_filter), message_thread_id=getattr(callback.message, 'message_thread_id', None))
  except Exception:
    logging.exception('admin qr browser nav photo send failed for item_id=%s qr=%r', item['id'] if hasattr(item, 'keys') else getattr(item, 'id', None), item['qr_file_id'] if hasattr(item, 'keys') else getattr(item, 'qr_file_id', None))
    await callback.message.answer(caption + "\n\n<i>QR preview недоступен.</i>", reply_markup=admin_qr_browser_kb(idx, total, operator_key, status_filter, day_filter, mode_filter))

def render_admin_queue_text(page: int = 0, operator_key: str = "all", day_filter: str = "all", mode_filter: str = "all") -> str:
  items, total = queue_page_items(page, 10, operator_key, day_filter, mode_filter)
  total_pages = max(1, (int(total) + 9) // 10)
  safe_page = min(max(0, int(page)), total_pages - 1)
  filters = []
  if operator_key != 'all':
    filters.append(escape(op_text(operator_key)))
  if day_filter == 'today':
    filters.append('сегодня')
  if mode_filter == 'hold':
    filters.append('холд')
  elif mode_filter == 'no_hold':
    filters.append('без холда')
  if not items:
    title = "<b>📦 Очередь</b>\n<b>Статус:</b> queued"
    if filters:
      title += f"\n<b>Фильтры:</b> {' • '.join(filters)}"
    return title + "\n\n<i>Заявок в очереди по выбранным фильтрам нет.</i>"
  rows = []
  for item in items:
    pos = queue_position(item['id']) if item['status'] == 'queued' else None
    pos_text = f" • позиция {pos}" if pos else ""
    rows.append(f"#{item['id']} • {op_text(item['operator_key'])} • {mode_label(item['mode'])} • {pretty_phone(item['normalized_phone'])}{pos_text}")
  header = f"<b>📦 Очередь</b>\n<b>Страница:</b> {safe_page + 1}/{total_pages}\n<b>Статус:</b> queued"
  if filters:
    header += f"\n<b>Фильтры:</b> {' • '.join(filters)}"
  return header + "\n\n" + quote_block(rows)

async def send_admin_queue_preview(target_message: Message, item_id: int, back_page: int = 0, back_operator: str = "all", back_day_filter: str = "all", back_mode_filter: str = "all"):
  item = db.get_queue_item(int(item_id))
  if not item:
    await target_message.answer("<b>👁 Просмотр заявки</b>\n\n<i>Заявка не найдена.</i>", reply_markup=queue_manage_kb(back_page, back_operator, back_day_filter, back_mode_filter))
    return
  caption = (
    f"<b>👁 Просмотр заявки без взятия</b>\n\n"
    f"<b>📌 ID:</b> <code>#{item['id']}</code>\n"
    f"<b>📱 Оператор:</b> {escape(op_text(item['operator_key']))}\n"
    f"<b>🧾 Режим:</b> {escape(mode_label(item['mode']))}\n"
    f"<b>☎️ Номер:</b> <code>{pretty_phone(item['normalized_phone'])}</code>\n"
    f"<b>📍 Статус:</b> {escape(status_label_from_row(item))}\n"
    f"<b>🕒 Создано:</b> {escape(item['created_at'])}"
  )
  try:
    await send_queue_item_photo_to_chat(target_message.bot, target_message.chat.id, item, caption=caption, reply_markup=queue_manage_kb(back_page, back_operator, back_day_filter, back_mode_filter), message_thread_id=getattr(target_message, 'message_thread_id', None))
  except Exception:
    logging.exception('admin queue preview photo failed')
    await target_message.answer(caption + "\n\n<i>QR preview недоступен.</i>", reply_markup=queue_manage_kb(back_page, back_operator, back_day_filter, back_mode_filter))

@router.callback_query(F.data == "admin:qr_numbers")
async def admin_qr_numbers(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await open_qr_browser_message(callback.message, 0, "all", "all", "all", "all")
  await callback.answer()

@router.callback_query(F.data.startswith("admin:qr_filter:"))
async def admin_qr_filter(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  parts = callback.data.split(":")
  operator_key = parts[2] if len(parts) > 2 else "all"
  status_filter = parts[3] if len(parts) > 3 else "all"
  day_filter = parts[4] if len(parts) > 4 else "all"
  try:
    idx = int(parts[5]) if len(parts) > 5 else 0
  except Exception:
    idx = 0
  mode_filter = parts[6] if len(parts) > 6 else "all"
  await update_qr_browser_message(callback, idx, operator_key, status_filter, day_filter, mode_filter)
  await callback.answer()

@router.callback_query(F.data.startswith("admin:qr_numbers:"))
async def admin_qr_numbers_nav(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  tail = callback.data.split(":")
  if len(tail) >= 3 and tail[2] == "noop":
    await callback.answer()
    return
  try:
    idx = int(tail[2]) if len(tail) > 2 else 0
  except Exception:
    idx = 0
  operator_key = tail[3] if len(tail) > 3 else "all"
  status_filter = tail[4] if len(tail) > 4 else "all"
  day_filter = tail[5] if len(tail) > 5 else "all"
  mode_filter = tail[6] if len(tail) > 6 else "all"
  await update_qr_browser_message(callback, idx, operator_key, status_filter, day_filter, mode_filter)
  await callback.answer()


@router.callback_query(F.data.startswith("admin:queue_jump:"))
async def admin_queue_jump_prompt(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  parts = callback.data.split(":")
  try:
    current_page = int(parts[2]) if len(parts) > 2 else 0
  except Exception:
    current_page = 0
  operator_key = parts[3] if len(parts) > 3 else "all"
  day_filter = parts[4] if len(parts) > 4 else "all"
  mode_filter = parts[5] if len(parts) > 5 else "all"
  items, total = queue_page_items(0, 10, operator_key, day_filter, mode_filter)
  total_pages = max(1, (int(total) + 9) // 10)
  await state.clear()
  await state.set_state(AdminStates.waiting_page_jump)
  await state.update_data(page_jump_target="queue", operator_key=operator_key, day_filter=day_filter, mode_filter=mode_filter, total_pages=total_pages)
  await callback.message.answer(
    f"<b>🔢 Переход к странице очереди</b>\n\n"
    f"Текущая страница: <b>{current_page + 1}</b>\n"
    f"Всего страниц: <b>{total_pages}</b>\n\n"
    f"Отправьте номер страницы цифрой."
  )
  await callback.answer()


@router.callback_query(F.data.startswith("admin:qr_jump:"))
async def admin_qr_jump_prompt(callback: CallbackQuery, state: FSMContext):
  if not is_admin(callback.from_user.id):
    return
  parts = callback.data.split(":")
  try:
    current_index = int(parts[2]) if len(parts) > 2 else 0
  except Exception:
    current_index = 0
  operator_key = parts[3] if len(parts) > 3 else "all"
  status_filter = parts[4] if len(parts) > 4 else "all"
  day_filter = parts[5] if len(parts) > 5 else "all"
  mode_filter = parts[6] if len(parts) > 6 else "all"
  rows, total = qr_page_items(0, 1, operator_key, status_filter, day_filter, mode_filter)
  total_pages = max(1, int(total))
  await state.clear()
  await state.set_state(AdminStates.waiting_page_jump)
  await state.update_data(page_jump_target="qr", operator_key=operator_key, status_filter=status_filter, day_filter=day_filter, mode_filter=mode_filter, total_pages=total_pages)
  await callback.message.answer(
    f"<b>🔢 Переход к странице QR</b>\n\n"
    f"Текущая страница: <b>{current_index + 1}</b>\n"
    f"Всего страниц: <b>{total_pages}</b>\n\n"
    f"Отправьте номер страницы цифрой."
  )
  await callback.answer()


@router.message(AdminStates.waiting_page_jump)
async def admin_page_jump_receive(message: Message, state: FSMContext):
  if not is_admin(message.from_user.id):
    await state.clear()
    return
  raw = (message.text or '').strip()
  if not raw.isdigit():
    await message.answer("Отправьте номер страницы цифрой, например: <code>3</code>")
    return
  page_number = int(raw)
  data = await state.get_data()
  total_pages = max(1, int(data.get("total_pages", 1)))
  operator_key = data.get("operator_key", "all")
  status_filter = data.get("status_filter", "all")
  day_filter = data.get("day_filter", "all")
  target = data.get("page_jump_target", "queue")
  mode_filter = data.get("mode_filter", "all")
  if page_number < 1 or page_number > total_pages:
    await message.answer(f"Страница должна быть от <b>1</b> до <b>{total_pages}</b>.")
    return
  await state.clear()
  page_index = page_number - 1
  if target == "qr":
    await open_qr_browser_message(message, page_index, operator_key, status_filter, day_filter, mode_filter)
  else:
    await message.answer(render_admin_queue_text(page_index, operator_key, day_filter, mode_filter), reply_markup=queue_manage_kb(page_index, operator_key, day_filter, mode_filter))

@router.callback_query(F.data == "admin:queues")
async def admin_queues(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  await safe_edit_or_send(callback, render_admin_queue_text(0, "all", "all", "all"), reply_markup=queue_manage_kb(0, "all", "all", "all"))
  await callback.answer()

@router.callback_query(F.data.startswith("admin:queue_filter:"))
async def admin_queue_filter(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  parts = callback.data.split(":")
  operator_key = parts[2] if len(parts) > 2 else "all"
  day_filter = parts[4] if len(parts) > 4 else "all"
  try:
    page = int(parts[5]) if len(parts) > 5 else 0
  except Exception:
    page = 0
  mode_filter = parts[6] if len(parts) > 6 else "all"
  await safe_edit_or_send(callback, render_admin_queue_text(page, operator_key, day_filter, mode_filter), reply_markup=queue_manage_kb(page, operator_key, day_filter, mode_filter))
  await callback.answer()

@router.callback_query(F.data.startswith("admin:queues:"))
async def admin_queues_nav(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  parts = callback.data.split(":")
  if len(parts) >= 3 and parts[2] == "noop":
    await callback.answer()
    return
  try:
    page = int(parts[2]) if len(parts) > 2 else 0
  except Exception:
    page = 0
  operator_key = parts[3] if len(parts) > 3 else "all"
  day_filter = parts[4] if len(parts) > 4 else "all"
  mode_filter = parts[5] if len(parts) > 5 else "all"
  await safe_edit_or_send(callback, render_admin_queue_text(page, operator_key, day_filter, mode_filter), reply_markup=queue_manage_kb(page, operator_key, day_filter, mode_filter))
  await callback.answer()

@router.callback_query(F.data.startswith("admin:queue_view:"))
async def admin_queue_view(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  parts = callback.data.split(":")
  try:
    item_id = int(parts[2])
  except Exception:
    await callback.answer("Некорректный ID", show_alert=True)
    return
  try:
    back_page = int(parts[3]) if len(parts) > 3 else 0
  except Exception:
    back_page = 0
  back_operator = parts[4] if len(parts) > 4 else "all"
  back_day_filter = parts[5] if len(parts) > 5 else "all"
  back_mode_filter = parts[6] if len(parts) > 6 else "all"
  await send_admin_queue_preview(callback.message, item_id, back_page, back_operator, back_day_filter, back_mode_filter)
  await callback.answer()

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
  await callback.answer()

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
  await callback.answer()

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
    ) or "• Данных пока нет"
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
  await callback.answer()
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
  await callback.answer()
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
  parts = callback.data.split(":")
  item_id = int(parts[2])
  try:
    page = int(parts[3]) if len(parts) > 3 else 0
  except Exception:
    page = 0
  operator_key = parts[4] if len(parts) > 4 else "all"
  day_filter = parts[5] if len(parts) > 5 else "all"
  remove_queue_item(item_id, reason='admin_removed', admin_id=callback.from_user.id)
  await safe_edit_or_send(callback, render_admin_queue_text(page, operator_key, day_filter), reply_markup=queue_manage_kb(page, operator_key, day_filter))
  await callback.answer("Удалено из очереди")

@router.callback_query(F.data.startswith("myremove:"))
async def myremove_cb(callback: CallbackQuery, state: FSMContext):
  item_id = int(callback.data.split(":")[-1])
  row = db.conn.execute("SELECT * FROM queue_items WHERE id=? AND user_id=?", (item_id, callback.from_user.id)).fetchone()
  if not row:
    await callback.answer("Заявка не найдена", show_alert=True)
    return
  if row["status"] != "queued":
    await callback.answer("Убрать можно только номер из очереди", show_alert=True)
    return
  remove_queue_item(item_id, reason='user_removed')
  items = user_active_queue_items(callback.from_user.id)
  await replace_banner_message(callback, db.get_setting('my_numbers_banner_path', MY_NUMBERS_BANNER), render_my_numbers(callback.from_user.id), my_numbers_kb(items))
  await send_log(callback.bot, f"<b>🗑 Удаление из очереди</b>\n👤 {escape(callback.from_user.full_name)}\n🆔 <code>{callback.from_user.id}</code>\n🧾 Заявка: <b>#{item_id}</b>")
  await callback.answer("Номер убран")

@router.callback_query(F.data.startswith("take_start:"))
async def take_start_cb(callback: CallbackQuery):
  if not is_operator_or_admin(callback.from_user.id):
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
      user_msg = await send_queue_item_photo_to_chat(callback.bot, int(fresh.user_id), fresh, queue_caption(fresh), message_thread_id=None)
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
        f"{mode_emoji(fresh.mode)} <b>Режим:</b> {mode_label(fresh.mode)}"
      )
  except Exception:
    pass
  await send_log(callback.bot, f"<b>🚀 Работа началась</b>\n👤 Взял: {escape(callback.from_user.full_name)}\n🆔 <code>{callback.from_user.id}</code>\n🧾 Заявка: <b>#{fresh.id}</b>\n📱 {op_html(fresh.operator_key)}\n📞 <code>{escape(pretty_phone(fresh.normalized_phone))}</code>\n🔄 {mode_label(fresh.mode)}")
  await callback.answer("Работа началась")

@router.callback_query(F.data.startswith("error_pre:"))
async def error_pre_cb(callback: CallbackQuery):
  if not is_operator_or_admin(callback.from_user.id):
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
  if not is_operator_or_admin(callback.from_user.id):
    return
  item_id = int(callback.data.split(":")[-1])
  item = db.get_queue_item(item_id)
  if not item or item.status != "in_progress" or item.mode != "no_hold":
    await callback.answer("Оплата недоступна", show_alert=True)
    return
  db.complete_queue_item(item_id)
  db.add_balance(item.user_id, float(item.price))
  referrer_id, ref_bonus = credit_referral_bonus(item.user_id, float(item.price))
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
  if not is_operator_or_admin(callback.from_user.id):
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
          caption=slip_text,
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
    await callback.answer()
    return
  if action == "set_price":
    await state.set_state(AdminStates.waiting_user_price_lookup)
    await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя для персонального прайса:</b>", reply_markup=ForceReply(selective=True))
    await callback.answer()
    return
  await state.update_data(user_action=action)
  await state.set_state(AdminStates.waiting_user_action_id)
  await callback.message.answer("<b>Введите ID, @username или сданный номер пользователя:</b>", reply_markup=ForceReply(selective=True))
  await callback.answer()

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
  ops_text = "\n".join([f"• {op_text(row['operator_key'])}: {row['total']} / {usd(row['earned'] or 0)}" for row in ops]) or "• Данных пока нет"
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
  await callback.answer()

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
  await callback.answer()

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
  await callback.answer()

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
  await state.clear()
  await state.set_state(AdminStates.waiting_db_upload)
  await message.answer("<b>📥 Загрузка базы</b>\n\nПришлите файл <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.\n\nЭтот режим полностью заменяет текущую базу. Для переноса только пользователей используйте команду <code>/importusersdb</code>.")

@router.message(Command("importusersdb"))
@router.message(Command("importusers"))
async def import_users_db_command(message: Message, state: FSMContext):
  if not is_admin(message.from_user.id):
    return
  await state.clear()
  await state.set_state(AdminStates.waiting_db_upload)
  await state.update_data(db_upload_mode="import_users")
  await message.answer("<b>👥 Импорт пользователей</b>\n\nПришлите старую SQLite базу. Я перенесу только пользователей в текущую базу, не ломая структуру нового бота.")

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
  data = await state.get_data()
  mode = data.get("db_upload_mode")
  try:
    if mode == "import_users":
      result = db.import_users_from_uploaded_db(str(temp_path))
      temp_path.unlink(missing_ok=True)
    else:
      backup_path = db.replace_with_uploaded_db(str(temp_path))
      try:
        db.reconnect()
      except Exception:
        logging.exception("db reconnect failed after upload")
      try:
        reload_operators_from_db()
      except Exception:
        logging.exception("reload_operators_from_db failed after upload")
  except Exception as e:
    logging.exception("db_upload_receive failed")
    temp_path.unlink(missing_ok=True)
    await message.answer(f"❌ Не удалось обработать базу. Причина: <code>{escape(str(e))}</code>")
    return
  await state.clear()
  if mode == "import_users":
    await message.answer(
      "<b>✅ Пользователи импортированы</b>\n\n"
      f"Всего найдено: <b>{result['total']}</b>\n"
      f"➕ Добавлено: <b>{result['imported']}</b>\n"
      f"♻️ Обновлено: <b>{result['updated']}</b>\n"
      f"⏭ Пропущено: <b>{result['skipped']}</b>"
    )
  else:
    await message.answer(
      "<b>✅ База загружена</b>\n\n"
      f"Текущая база заменена сразу. Резервная копия: <code>{escape(str(backup_path))}</code>"
    )

@router.message(AdminStates.waiting_db_upload)
async def db_upload_wrong(message: Message):
  await message.answer("Пришлите файл базы <code>.db</code>, <code>.sqlite</code> или <code>.sqlite3</code>.")


@router.message(Command("stata"))
@router.message(Command("Stata"))
async def group_stata(message: Message):
  role = user_role(message.from_user.id)
  if role not in {"chief_admin", "admin", "operator"}:
    return
  if message.chat.type == ChatType.PRIVATE:
    await message.answer("Статистику групп смотрите через кнопку в /admin.")
    return
  try:
    day_start, day_end, day_label = msk_today_bounds_str()
    chat_id = message.chat.id
    thread_id = getattr(message, "message_thread_id", None)
    thread_key = db._thread_key(thread_id)

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
      WHERE charge_chat_id=? AND charge_thread_id=? AND taken_at>=? AND taken_at<?
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
      WHERE charge_chat_id=? AND charge_thread_id=? AND taken_at>=? AND taken_at<?
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
    if role in {"chief_admin", "admin"}:
      lines.extend([
        f"🏦 Списано с казны: <b>{usd(totals['spent_total'] or 0)}</b>",
      ])
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
    await message.answer("⚠️ Не удалось собрать статистику группы. Смотрите её через кнопку в /admin.")

@router.callback_query(F.data == "admin:set_withdraw_channel")
async def admin_set_withdraw_channel(callback: CallbackQuery, state: FSMContext):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  await state.update_data(channel_target="withdraw_channel_id")
  await state.set_state(AdminStates.waiting_channel_value)
  current_value = escape(db.get_setting("withdraw_channel_id", str(WITHDRAW_CHANNEL_ID)))
  await callback.message.answer(
    "Введите новый <b>ID канала выплат</b>:\n"
    f"Текущее значение: <code>{current_value}</code>"
  )
  await callback.answer()


@router.callback_query(F.data == "admin:set_withdraw_topic")
async def admin_set_withdraw_topic(callback: CallbackQuery, state: FSMContext):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  await state.update_data(channel_target="withdraw_thread_id")
  await state.set_state(AdminStates.waiting_channel_value)
  current_value = escape(db.get_setting("withdraw_thread_id", "0"))
  await callback.message.answer(
    "Введите новый <b>ID топика выплат</b>:\n"
    "Отправь <code>0</code>, чтобы отключить топик и слать выплаты просто в канал.\n"
    f"Текущее значение: <code>{current_value}</code>"
  )
  await callback.answer()


@router.callback_query(F.data == "admin:set_backup_channel")
async def admin_set_backup_channel(callback: CallbackQuery, state: FSMContext):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  await state.update_data(channel_target="backup_channel_id")
  await state.set_state(AdminStates.waiting_channel_value)
  current_value = escape(db.get_setting("backup_channel_id", "0"))
  await callback.message.answer(
    "Введите новый <b>ID канала автобэкапа</b>:\n"
    f"Текущее значение: <code>{current_value}</code>"
  )
  await callback.answer()


@router.callback_query(F.data == "admin:toggle_backup")
async def admin_toggle_backup(callback: CallbackQuery):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  enabled = not is_backup_enabled()
  set_backup_enabled(enabled)
  await safe_edit_or_send(callback, render_admin_settings(), reply_markup=settings_kb())
  await callback.answer("Автовыгрузка включена" if enabled else "Автовыгрузка выключена")


@router.callback_query(F.data == "admin:set_log_channel")
async def admin_set_log_channel(callback: CallbackQuery, state: FSMContext):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  await state.update_data(channel_target="log_channel_id")
  await state.set_state(AdminStates.waiting_channel_value)
  await callback.message.answer("Введите новый <b>ID канала логов</b>:")
  await callback.answer()

@router.callback_query(F.data == "admin:required_join_manage")
async def admin_required_join_manage(callback: CallbackQuery):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  await safe_edit_or_send(callback, render_required_join_admin(), reply_markup=required_join_manage_kb())
  await callback.answer()

@router.callback_query(F.data == "admin:required_join_add")
async def admin_required_join_add(callback: CallbackQuery, state: FSMContext):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  await state.set_state(AdminStates.waiting_required_join_item)
  await callback.message.answer(
    "Пришли новый канал в формате:\n"
    "<code>-100xxxxxxxxxx | https://t.me/your_link | Название</code>\n\n"
    "Название можно не указывать. Для отмены отправь <code>-</code>."
  )
  await callback.answer()

@router.callback_query(F.data == "admin:required_join_remove")
async def admin_required_join_remove(callback: CallbackQuery, state: FSMContext):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  items = required_join_entries()
  if not items:
    await callback.answer("Список пуст", show_alert=True)
    return
  await state.set_state(AdminStates.waiting_required_join_remove)
  lines = ["Что убрать? Отправь <b>номер из списка</b> или <b>ID канала</b>.", ""]
  for idx, item in enumerate(items, 1):
    title = escape(item.get("title") or f"Канал {idx}")
    lines.append(f"{idx}. {title} — <code>{item['chat_id']}</code>")
  await callback.message.answer("\n".join(lines))
  await callback.answer()

@router.callback_query(F.data == "admin:required_join_clear")
async def admin_required_join_clear(callback: CallbackQuery):
  if not is_chief_admin(callback.from_user.id):
    await callback.answer("Только главный админ", show_alert=True)
    return
  save_required_join_entries([])
  await safe_edit_or_send(callback, render_required_join_admin(), reply_markup=required_join_manage_kb())
  await callback.answer("Список очищен")

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
  if user_role(message.from_user.id) != "chief_admin":
    await state.clear()
    return
  raw = message.text.strip()
  if not raw.lstrip("-").isdigit():
    await message.answer("Введите ID канала числом.")
    return
  data = await state.get_data()
  key = data.get("channel_target")
  db.set_setting(key, raw)
  await state.clear()
  await message.answer("✅ Сохранено.")




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
  await callback.answer()

@router.callback_query(F.data.startswith("admin:groupfin:"))
async def admin_group_finance_open(callback: CallbackQuery):
  if not is_admin(callback.from_user.id):
    return
  _, _, chat_id, thread_id = callback.data.split(":")
  chat_id = int(chat_id)
  thread_id = None if int(thread_id) == 0 else int(thread_id)
  await safe_edit_or_send(callback, render_group_finance(chat_id, thread_id), reply_markup=group_finance_manage_kb(chat_id, thread_id))
  await callback.answer()

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
  await callback.answer()

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
  await callback.answer()

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


async def main():
  global LIVE_DP, PRIMARY_BOT
  if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
    raise RuntimeError("Укажи BOT_TOKEN прямо в bot.py")

  db.recover_after_restart()
  primary_bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
  PRIMARY_BOT = primary_bot
  dp = Dispatcher(storage=MemoryStorage())
  dp.include_router(router)
  LIVE_DP = dp

  asyncio.create_task(hold_watcher(primary_bot))
  asyncio.create_task(backup_watcher(primary_bot))

  await run_web_server()

  try:
    me = await primary_bot.get_me()
    db.set_setting('bot_username_cached', me.username or BOT_USERNAME_FALLBACK)
    if WEBAPP_BASE_URL:
      try:
        await primary_bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="DVE", web_app=WebAppInfo(url=miniapp_url('/'))))
      except Exception:
        logging.exception("set_chat_menu_button failed")
    logging.info("Primary bot started as @%s", me.username or BOT_USERNAME_FALLBACK)
    logging.info("Anti-crash recovery complete; holds and queue state restored")
  except Exception:
    logging.exception("Primary bot get_me failed")

  for mirror in db.all_active_mirrors():
    token = (mirror["token"] or "").strip()
    if not token or token == BOT_TOKEN:
      continue
    await start_live_mirror(token)

  await dp.start_polling(primary_bot)


if __name__ == "__main__":
  asyncio.run(main())
