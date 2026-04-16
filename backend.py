import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from dotenv import load_dotenv

from repositories import (
    cashflow_repository,
    daily_nav_snapshot_repository,
    macro_journal_repository,
    manual_price_override_repository,
    market_holiday_repository,
    notebook_article_repository,
    portfolio_repository,
    price_snapshot_repository,
    stock_name_repository,
    trade_repository,
    trade_cycle_review_repository,
)
from supabase_db import get_db_connection, get_supabase_client

load_dotenv()

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
TWSE_HOLIDAY_URL = "https://www.twse.com.tw/holidaySchedule/holidaySchedule"
_session = requests.Session()
_session.headers.update(
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
)
TRADE_RISK_META_PREFIX = "[[TRADE_RISK]]"
TW_MARKET_CLOSE_HOUR = 13
TW_MARKET_CLOSE_MINUTE = 30
TW_MARKET_FINALIZATION_DELAY_MINUTES = 30


def normalize_stock_id(s):
    s = str(s).strip().upper()
    if s.endswith(".TW"):
        s = s[:-3]
    if s.endswith(".TWO"):
        s = s[:-4]
    return re.split(r"[\s\-]", s)[0].strip()


def get_tw_now():
    return datetime.now()

def ensure_db_schema():
    conn = get_db_connection()
    try:
        portfolio_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(Portfolios)").fetchall()
        }
        if "initial_cash" not in portfolio_cols:
            conn.execute(
                "ALTER TABLE Portfolios ADD COLUMN initial_cash REAL NOT NULL DEFAULT 0.0"
            )
        if "nav_dirty_from_date" not in portfolio_cols:
            conn.execute(
                "ALTER TABLE Portfolios ADD COLUMN nav_dirty_from_date TEXT"
            )

        trade_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(Trades)").fetchall()
        }
        if "settled" not in trade_cols:
            conn.execute(
                "ALTER TABLE Trades ADD COLUMN settled INTEGER NOT NULL DEFAULT 0"
            )
            conn.execute(
                """
                UPDATE Trades
                SET settled = CASE
                    WHEN is_disposed = 1 OR action = 'Setup' THEN 1
                    ELSE 0
                END
                """
            )
        for score_col, default_value in {
            "technical_score": 3,
            "chip_score": 3,
            "theme_score": 3,
        }.items():
            if score_col not in trade_cols:
                conn.execute(
                    f"ALTER TABLE Trades ADD COLUMN {score_col} INTEGER NOT NULL DEFAULT {default_value}"
                )
        for risk_col in ["take_profit_price", "stop_loss_price"]:
            if risk_col not in trade_cols:
                conn.execute(f"ALTER TABLE Trades ADD COLUMN {risk_col} REAL")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS StockNames (
                stock_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                full_symbol TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS MarketHolidays (
                date TEXT PRIMARY KEY,
                reason TEXT,
                is_settlement_open INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        holiday_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(MarketHolidays)").fetchall()
        }
        if "is_settlement_open" not in holiday_cols:
            conn.execute(
                "ALTER TABLE MarketHolidays ADD COLUMN is_settlement_open INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ManualPriceOverrides (
                stock_id TEXT PRIMARY KEY,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS PriceSnapshots (
                date TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                close_price REAL NOT NULL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (date, stock_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS DailyNavSnapshots (
                portfolio_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                nav REAL NOT NULL,
                cash REAL NOT NULL,
                holdings_value REAL NOT NULL,
                cash_flow REAL NOT NULL,
                daily_return REAL NOT NULL,
                twr REAL NOT NULL,
                benchmark_price REAL NOT NULL,
                benchmark_daily_return REAL NOT NULL,
                benchmark_cum_return REAL NOT NULL,
                daily_alpha REAL NOT NULL,
                cum_alpha REAL NOT NULL,
                drawdown REAL NOT NULL,
                notes TEXT,
                allocation_json TEXT,
                is_trading_day INTEGER NOT NULL DEFAULT 1,
                win_flag TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (portfolio_id, date),
                FOREIGN KEY (portfolio_id) REFERENCES Portfolios(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS TradeCycleReviews (
                portfolio_id INTEGER NOT NULL,
                stock_id TEXT NOT NULL,
                cycle_no INTEGER NOT NULL,
                review_text TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (portfolio_id, stock_id, cycle_no),
                FOREIGN KEY (portfolio_id) REFERENCES Portfolios(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS NotebookArticles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                note_type TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                created_date TEXT NOT NULL,
                update_log_json TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL
            )
            """
        )
        nav_snapshot_cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(DailyNavSnapshots)").fetchall()
        }
        if "allocation_json" not in nav_snapshot_cols:
            conn.execute(
                "ALTER TABLE DailyNavSnapshots ADD COLUMN allocation_json TEXT"
            )
            conn.execute(
                """
                UPDATE Portfolios
                SET nav_dirty_from_date = (
                    SELECT MIN(date)
                    FROM DailyNavSnapshots dns
                    WHERE dns.portfolio_id = Portfolios.id
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM DailyNavSnapshots dns
                    WHERE dns.portfolio_id = Portfolios.id
                )
                  AND nav_dirty_from_date IS NULL
                """
            )
        # 先保留固定種子，若 TWSE 行事曆同步失敗，至少不會漏掉已知連假。
        conn.executemany(
            """
            INSERT OR IGNORE INTO MarketHolidays (date, reason, is_settlement_open)
            VALUES (?, ?, ?)
            """,
            [
                ("2026-04-03", "連假休市", 0),
                ("2026-04-06", "連假休市", 0),
            ],
        )
        # 舊版預設帳戶只有 Portfolios 現金，沒有 CashFlows 紀錄，會導致歷史 NAV 從 0 開始。
        conn.execute(
            """
            UPDATE Portfolios
            SET initial_cash = CASE
                WHEN name = '自己' THEN 1000000.0
                WHEN name = '爸爸' THEN 500000.0
                ELSE initial_cash
            END
            WHERE COALESCE(initial_cash, 0) = 0
              AND name IN ('自己', '爸爸')
              AND NOT EXISTS (
                  SELECT 1 FROM CashFlows cf WHERE cf.portfolio_id = Portfolios.id
              )
            """
        )
        conn.commit()
    finally:
        conn.close()

    sync_twse_market_holidays()


def get_portfolios():
    df = portfolio_repository.list_portfolios()
    if not df.empty and "id" in df.columns:
        df = df.sort_values("id", kind="stable").reset_index(drop=True)
    return df


def get_portfolio_state(portfolio_id):
    state = portfolio_repository.get_state(portfolio_id)
    return state["t0_cash"], state["t2_cash"]


def create_portfolio(name, initial_cash=0.0):
    try:
        return portfolio_repository.create_portfolio(name, initial_cash)
    except Exception as exc:
        err_text = str(exc).lower()
        if "duplicate" in err_text or "23505" in err_text or "unique" in err_text:
            raise sqlite3.IntegrityError(str(exc)) from exc
        raise


def delete_portfolio_and_related_data(portfolio_id):
    trade_cycle_review_repository.delete_portfolio_reviews(portfolio_id)
    daily_nav_snapshot_repository.delete_portfolio_snapshots(portfolio_id)
    trade_repository.delete_portfolio_trades(portfolio_id)
    cashflow_repository.delete_portfolio_cashflows(portfolio_id)
    portfolio_repository.delete_portfolio(portfolio_id)


def get_portfolio_trades_df(portfolio_id):
    trades_df = trade_repository.list_trades(portfolio_id)
    if trades_df.empty:
        return trades_df

    for risk_col in ["take_profit_price", "stop_loss_price"]:
        if risk_col not in trades_df.columns:
            trades_df[risk_col] = np.nan

    for idx, row in trades_df.iterrows():
        row_tp = _normalize_optional_price_target(row.get("take_profit_price"))
        row_sl = _normalize_optional_price_target(row.get("stop_loss_price"))
        if row_tp is not None:
            trades_df.at[idx, "take_profit_price"] = row_tp
        if row_sl is not None:
            trades_df.at[idx, "stop_loss_price"] = row_sl
        if row_tp is not None and row_sl is not None:
            continue

        fallback_tp, fallback_sl = _parse_trade_risk_targets_from_notes(
            row.get("trading_notes", "")
        )
        if row_tp is None and fallback_tp is not None:
            trades_df.at[idx, "take_profit_price"] = fallback_tp
        if row_sl is None and fallback_sl is not None:
            trades_df.at[idx, "stop_loss_price"] = fallback_sl
    return trades_df


def get_inventory(portfolio_id, stock_id=None):
    trades_df = trade_repository.list_trades(
        portfolio_id, columns="stock_id,action,shares"
    )
    if trades_df.empty:
        return pd.DataFrame(columns=["stock_id", "current_shares"])

    trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)
    if stock_id:
        trades_df = trades_df[trades_df["stock_id"] == normalize_stock_id(stock_id)].copy()
    if trades_df.empty:
        return pd.DataFrame(columns=["stock_id", "current_shares"])

    trade_signs = np.where(
        trades_df["action"].isin(["Buy", "Add", "Setup"]),
        pd.to_numeric(trades_df["shares"], errors="coerce").fillna(0),
        np.where(
            trades_df["action"].isin(["Reduce", "Close"]),
            -pd.to_numeric(trades_df["shares"], errors="coerce").fillna(0),
            0,
        ),
    )
    trades_df["signed_shares"] = trade_signs
    return (
        trades_df.groupby("stock_id", as_index=False)["signed_shares"]
        .sum()
        .rename(columns={"signed_shares": "current_shares"})
        .query("current_shares > 0")
        .reset_index(drop=True)
    )


def get_inventory_as_of_date(portfolio_id, stock_id=None, as_of_date=None):
    trades_df = trade_repository.list_trades(
        portfolio_id, columns="date,stock_id,action,shares,id"
    )
    if trades_df.empty:
        return pd.DataFrame(columns=["stock_id", "current_shares"])

    trades_df = trades_df.copy()
    trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)
    trades_df["date"] = pd.to_datetime(trades_df["date"], errors="coerce")
    trades_df["shares"] = pd.to_numeric(trades_df["shares"], errors="coerce").fillna(0)
    trades_df = trades_df.dropna(subset=["date"])

    if stock_id:
        trades_df = trades_df[
            trades_df["stock_id"] == normalize_stock_id(stock_id)
        ].copy()
    if trades_df.empty:
        return pd.DataFrame(columns=["stock_id", "current_shares"])

    if as_of_date is not None:
        cutoff = pd.to_datetime(as_of_date, errors="coerce")
        if not pd.isna(cutoff):
            trades_df = trades_df[trades_df["date"] < cutoff.normalize()].copy()
    if trades_df.empty:
        return pd.DataFrame(columns=["stock_id", "current_shares"])

    trade_signs = np.where(
        trades_df["action"].isin(["Buy", "Add", "Setup"]),
        trades_df["shares"],
        np.where(
            trades_df["action"].isin(["Reduce", "Close"]),
            -trades_df["shares"],
            0,
        ),
    )
    trades_df["signed_shares"] = trade_signs
    return (
        trades_df.groupby("stock_id", as_index=False)["signed_shares"]
        .sum()
        .rename(columns={"signed_shares": "current_shares"})
        .query("current_shares > 0")
        .reset_index(drop=True)
    )


def _normalize_ai_trade_side(raw_value):
    text = str(raw_value or "").strip().lower()
    if not text:
        return ""
    if text in {"buy", "b", "bid", "long", "買", "買進", "買入"}:
        return "buy"
    if text in {"sell", "s", "ask", "short", "賣", "賣出", "賣掉"}:
        return "sell"
    return ""


def _normalize_ai_trade_date(raw_value):
    parsed = pd.to_datetime(raw_value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def get_market_holiday_dates():
    holiday_df = market_holiday_repository.list_holidays()
    if holiday_df.empty:
        return set()
    return set(holiday_df["date"].astype(str).tolist())


def get_settlement_blocked_dates():
    holiday_df = market_holiday_repository.list_holidays()
    if holiday_df.empty:
        return set()
    holiday_df["is_settlement_open"] = holiday_df["is_settlement_open"].fillna(False).astype(bool)
    return set(holiday_df.loc[~holiday_df["is_settlement_open"], "date"].astype(str).tolist())


def get_market_holidays_df():
    df = market_holiday_repository.list_holidays()
    if not df.empty:
        df["reason"] = df["reason"].fillna("")
        df["is_settlement_open"] = df["is_settlement_open"].fillna(0).astype(bool)
    return df


def upsert_market_holiday(date_str, reason="", is_settlement_open=False):
    market_holiday_repository.upsert_holiday(date_str, reason, is_settlement_open)


def delete_market_holiday(date_str):
    market_holiday_repository.delete_holiday(date_str)


def is_tw_market_open(date_value, holiday_dates=None):
    d = pd.to_datetime(date_value)
    if d.weekday() >= 5:
        return False
    if holiday_dates is None:
        holiday_dates = get_market_holiday_dates()
    return d.strftime("%Y-%m-%d") not in holiday_dates


def get_latest_tw_trading_date(date_value=None, holiday_dates=None):
    current = pd.to_datetime(date_value if date_value is not None else datetime.now())
    if holiday_dates is None:
        holiday_dates = get_market_holiday_dates()
    while not is_tw_market_open(current, holiday_dates):
        current -= timedelta(days=1)
    return current.strftime("%Y-%m-%d")


def get_latest_official_tw_trading_date(date_value=None, holiday_dates=None):
    current = pd.to_datetime(date_value if date_value is not None else get_tw_now())
    if holiday_dates is None:
        holiday_dates = get_market_holiday_dates()

    if not is_tw_market_open(current, holiday_dates):
        return get_latest_tw_trading_date(current, holiday_dates)

    official_cutoff = current.normalize() + pd.Timedelta(
        hours=TW_MARKET_CLOSE_HOUR,
        minutes=TW_MARKET_CLOSE_MINUTE + TW_MARKET_FINALIZATION_DELAY_MINUTES,
    )
    if current < official_cutoff:
        current -= timedelta(days=1)
    return get_latest_tw_trading_date(current, holiday_dates)


def estimate_settlement_date(trade_date, business_days=2, holiday_dates=None):
    current = pd.to_datetime(trade_date)
    if holiday_dates is None:
        holiday_dates = get_settlement_blocked_dates()
    added = 0
    while added < business_days:
        current += timedelta(days=1)
        if current.weekday() < 5 and current.strftime("%Y-%m-%d") not in holiday_dates:
            added += 1
    return current.strftime("%Y-%m-%d")


def _is_twse_trading_marker(name_text, desc_text):
    merged_text = f"{name_text or ''} {desc_text or ''}"
    if "市場無交易" in merged_text:
        return False
    return ("開始交易" in merged_text) or ("最後交易" in merged_text)


@st.cache_data(ttl=86400)
def fetch_twse_market_holiday_rows(target_year):
    query_year = int(target_year) - 1911
    response = _session.get(
        TWSE_HOLIDAY_URL,
        params={"response": "html", "queryYear": str(query_year)},
        timeout=10,
    )
    response.raise_for_status()

    try:
        holiday_tables = pd.read_html(response.text)
    except Exception:
        holiday_tables = []
    if not holiday_tables:
        return []

    raw_df = holiday_tables[0].copy()
    if raw_df.empty:
        return []

    date_col = next(
        (col for col in raw_df.columns if "日期" in str(col)),
        raw_df.columns[0],
    )
    name_col = next(
        (col for col in raw_df.columns if "名稱" in str(col)),
        raw_df.columns[1] if len(raw_df.columns) > 1 else raw_df.columns[0],
    )
    desc_col = next(
        (col for col in raw_df.columns if "說明" in str(col)),
        raw_df.columns[2] if len(raw_df.columns) > 2 else raw_df.columns[-1],
    )

    parsed_rows = []
    for _, row in raw_df.iterrows():
        holiday_date = pd.to_datetime(row.get(date_col), errors="coerce")
        if pd.isna(holiday_date):
            continue

        name_text = str(row.get(name_col) or "").strip()
        desc_text = str(row.get(desc_col) or "").strip()
        if _is_twse_trading_marker(name_text, desc_text):
            continue

        is_settlement_open = "僅辦理結算交割作業" in f"{name_text} {desc_text}"
        reason_text = "｜".join(
            [txt for txt in [name_text, desc_text] if txt and txt.lower() != "nan"]
        )
        parsed_rows.append(
            {
                "date": holiday_date.strftime("%Y-%m-%d"),
                "reason": reason_text or "休市",
                "is_settlement_open": int(is_settlement_open),
            }
        )

    return parsed_rows


def sync_twse_market_holidays(target_years=None):
    if target_years is None:
        current_year = datetime.now().year
        target_years = [current_year - 1, current_year, current_year + 1]

    all_rows = []
    for year_value in dict.fromkeys(int(y) for y in target_years):
        try:
            all_rows.extend(fetch_twse_market_holiday_rows(year_value))
        except Exception:
            continue

    if not all_rows:
        return 0

    return market_holiday_repository.upsert_many(all_rows)


def _signed_trade_amount(action, amount):
    return -amount if action in ["Buy", "Add"] else amount


def build_pending_settlement_schedule(portfolio_id):
    pending_df = trade_repository.list_unsettled_trades(portfolio_id)
    if not pending_df.empty:
        pending_df = pending_df.rename(
            columns={"date": "交易日", "stock_id": "標的", "action": "動作"}
        )
        pending_df["原始金額"] = (
            pd.to_numeric(pending_df["price"], errors="coerce").fillna(0)
            * pd.to_numeric(pending_df["shares"], errors="coerce").fillna(0)
        )

    if pending_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    pending_df["標的"] = pending_df["標的"].apply(normalize_stock_id)
    holiday_dates = get_settlement_blocked_dates()
    pending_df["預計交割日"] = pending_df["交易日"].apply(
        lambda d: estimate_settlement_date(d, holiday_dates=holiday_dates)
    )
    pending_df["交割金額"] = pending_df.apply(
        lambda r: _signed_trade_amount(r["動作"], r["原始金額"]), axis=1
    )

    summary_df = (
        pending_df.groupby("預計交割日", as_index=False)
        .agg(
            淨交割額=("交割金額", "sum"),
            筆數=("交割金額", "size"),
        )
        .sort_values("預計交割日")
        .reset_index(drop=True)
    )
    summary_df["交割方向"] = summary_df["淨交割額"].apply(
        lambda x: "應收" if x > 0 else ("應付" if x < 0 else "持平")
    )
    pending_df["交割方向"] = pending_df["交割金額"].apply(
        lambda x: "應收" if x > 0 else ("應付" if x < 0 else "持平")
    )
    return pending_df, summary_df


def mark_nav_snapshots_dirty(portfolio_id, from_date):
    dirty_date = pd.to_datetime(from_date, errors="coerce")
    if pd.isna(dirty_date):
        return
    dirty_str = dirty_date.strftime("%Y-%m-%d")

    current_dirty = portfolio_repository.get_nav_dirty_from_date(portfolio_id)
    if current_dirty:
        dirty_str = min(current_dirty, dirty_str)
    daily_nav_snapshot_repository.delete_from_date(portfolio_id, dirty_str)
    portfolio_repository.set_nav_dirty_from_date(portfolio_id, dirty_str)


def _normalize_optional_price_target(target_price):
    numeric_value = pd.to_numeric(target_price, errors="coerce")
    if pd.isna(numeric_value) or float(numeric_value) <= 0:
        return None
    return round(float(numeric_value), 4)


def _parse_trade_risk_targets_from_notes(notes_text):
    text = str(notes_text or "")
    for raw_line in text.splitlines():
        line_text = raw_line.strip()
        if not line_text.startswith(TRADE_RISK_META_PREFIX):
            continue
        try:
            payload = json.loads(line_text[len(TRADE_RISK_META_PREFIX):].strip())
        except Exception:
            return None, None
        return (
            _normalize_optional_price_target(payload.get("take_profit_price")),
            _normalize_optional_price_target(payload.get("stop_loss_price")),
        )
    return None, None


def _compose_trade_notes_with_risk(notes_text, take_profit_price=None, stop_loss_price=None):
    cleaned_lines = [
        line
        for line in str(notes_text or "").splitlines()
        if not line.strip().startswith(TRADE_RISK_META_PREFIX)
    ]
    cleaned_notes = "\n".join(cleaned_lines).rstrip()
    tp_target = _normalize_optional_price_target(take_profit_price)
    sl_target = _normalize_optional_price_target(stop_loss_price)
    if tp_target is None and sl_target is None:
        return cleaned_notes

    payload = {
        "take_profit_price": tp_target,
        "stop_loss_price": sl_target,
    }
    marker_line = f"{TRADE_RISK_META_PREFIX} {json.dumps(payload, ensure_ascii=False)}"
    return f"{cleaned_notes}\n{marker_line}".strip() if cleaned_notes else marker_line


def _find_stale_allocation_snapshot_date(existing_nav_df, valid_stock_ids):
    if existing_nav_df is None or existing_nav_df.empty:
        return None

    valid_assets = set(normalize_stock_id(sid) for sid in (valid_stock_ids or []) if str(sid).strip())
    valid_assets.add("可用現金 (T+0)")

    for _, row in existing_nav_df.sort_values("Date", kind="stable").iterrows():
        try:
            allocation_obj = json.loads(row.get("AllocationJson", "{}") or "{}")
        except Exception:
            continue

        unknown_assets = [
            normalize_stock_id(asset_key)
            for asset_key in allocation_obj.keys()
            if asset_key != "可用現金 (T+0)"
            and normalize_stock_id(asset_key) not in valid_assets
        ]
        if unknown_assets:
            stale_date = pd.to_datetime(row.get("Date"), errors="coerce")
            return stale_date.strftime("%Y-%m-%d") if not pd.isna(stale_date) else None
    return None


def _extract_position_meta(processed_trade_df, fallback_date=None):
    if processed_trade_df is None or processed_trade_df.empty:
        eval_date = pd.to_datetime(fallback_date or datetime.now(), errors="coerce")
        if pd.isna(eval_date):
            eval_date = pd.to_datetime(datetime.now())
        return eval_date.strftime("%Y-%m-%d"), 0, None, None

    sorted_df = processed_trade_df.copy()
    sorted_df["date"] = pd.to_datetime(sorted_df["date"], errors="coerce")
    sorted_df = sorted_df.dropna(subset=["date"]).sort_values("date", kind="stable").reset_index(drop=True)
    if sorted_df.empty:
        eval_date = pd.to_datetime(fallback_date or datetime.now(), errors="coerce")
        if pd.isna(eval_date):
            eval_date = pd.to_datetime(datetime.now())
        return eval_date.strftime("%Y-%m-%d"), 0, None, None

    segment_start_date = sorted_df.iloc[0]["date"]
    latest_take_profit = None
    latest_stop_loss = None
    current_shares = 0

    for _, row in sorted_df.iterrows():
        action = str(row.get("action", "")).strip()
        shares = int(row.get("shares", 0) or 0)
        if action in ["Buy", "Setup"] and current_shares <= 0:
            segment_start_date = row["date"]
            latest_take_profit = None
            latest_stop_loss = None
        if action in ["Buy", "Add", "Setup"]:
            current_shares += shares
        elif action in ["Reduce", "Close"]:
            current_shares = max(0, current_shares - shares)
            if current_shares == 0:
                latest_take_profit = None
                latest_stop_loss = None

        tp_target = _normalize_optional_price_target(row.get("take_profit_price"))
        sl_target = _normalize_optional_price_target(row.get("stop_loss_price"))
        if tp_target is None and sl_target is None:
            tp_target, sl_target = _parse_trade_risk_targets_from_notes(
                row.get("trading_notes", "")
            )
        if tp_target is not None:
            latest_take_profit = tp_target
        if sl_target is not None:
            latest_stop_loss = sl_target

    eval_date = pd.to_datetime(fallback_date or datetime.now(), errors="coerce")
    if pd.isna(eval_date):
        eval_date = pd.to_datetime(datetime.now())
    holding_days = max(1, int((eval_date.normalize() - segment_start_date.normalize()).days) + 1)
    return segment_start_date.strftime("%Y-%m-%d"), holding_days, latest_take_profit, latest_stop_loss


def validate_trade_request(portfolio_id, stock_id, action, shares):
    stock_id = normalize_stock_id(stock_id)
    action = str(action).strip()
    shares = int(shares or 0)

    if shares <= 0:
        raise ValueError("股數必須大於 0。")

    if action == "Setup":
        return

    inv_df = get_inventory(portfolio_id, stock_id)
    current_shares = int(inv_df.iloc[0]["current_shares"]) if not inv_df.empty else 0

    if current_shares <= 0:
        if action != "Buy":
            raise ValueError(
                f"{stock_id} 目前沒有庫存，不能使用「{action}」。若是首次建立部位，請改選 Buy。"
            )
        return

    if action == "Buy":
        raise ValueError(
            f"{stock_id} 目前已持有 {current_shares} 股，不能再用「Buy 首次買進」。請改用 Add / Reduce / Close。"
        )

    if action == "Add":
        return

    if action == "Reduce":
        if shares >= current_shares:
            raise ValueError(
                f"{stock_id} 目前持有 {current_shares} 股，Reduce 股數必須小於現有股數；如果要全數出場，請改用 Close 並填 {current_shares} 股。"
            )
        return

    if action == "Close":
        if shares != current_shares:
            raise ValueError(
                f"{stock_id} 目前持有 {current_shares} 股，Close 必須剛好賣出全部庫存；若只要部分減碼，請改用 Reduce。"
            )
        return

    raise ValueError(f"不支援的交易動作：{action}")


def assert_trade_date_is_open(trade_date):
    parsed_date = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(parsed_date):
        raise ValueError("交易日期格式不正確。")
    if not is_tw_market_open(parsed_date):
        raise ValueError(
            f"{parsed_date.strftime('%Y-%m-%d')} 是台股休市日，不能新增或修改交易。"
        )


def validate_trade_timeline(portfolio_id, candidate_trade=None, replaced_trade_id=None):
    trades_df = trade_repository.list_trades(portfolio_id)

    if replaced_trade_id is not None and not trades_df.empty:
        trades_df = trades_df[trades_df["id"] != int(replaced_trade_id)].copy()

    if candidate_trade is not None:
        trades_df = pd.concat(
            [trades_df, pd.DataFrame([candidate_trade])],
            ignore_index=True,
        )

    if trades_df.empty:
        return

    trades_df["date"] = pd.to_datetime(trades_df["date"], errors="coerce")
    trades_df["id"] = pd.to_numeric(trades_df["id"], errors="coerce").fillna(10**15)
    trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)
    trades_df = trades_df.dropna(subset=["date"]).sort_values(
        ["date", "id"], kind="stable"
    )

    inventory = {}
    for _, row in trades_df.iterrows():
        action = str(row.get("action", "")).strip()
        stock_id = normalize_stock_id(row.get("stock_id", ""))
        shares = int(row.get("shares", 0) or 0)
        if action == "Dividend":
            continue
        if shares <= 0:
            raise ValueError(
                f"{pd.to_datetime(row['date']).strftime('%Y-%m-%d')} {stock_id} 的股數必須大於 0。"
            )

        current_shares = int(inventory.get(stock_id, 0))
        if action == "Setup":
            if current_shares > 0:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} 前已經有庫存，不能再插入 Setup。"
                )
            inventory[stock_id] = shares
        elif action == "Buy":
            if current_shares > 0:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} 前已持有 {current_shares} 股，不能使用 Buy。"
                )
            inventory[stock_id] = shares
        elif action == "Add":
            if current_shares <= 0:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} 前沒有庫存，不能使用 Add。"
                )
            inventory[stock_id] = current_shares + shares
        elif action == "Reduce":
            if current_shares <= 0:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} 前沒有庫存，不能使用 Reduce。"
                )
            if shares >= current_shares:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} 減碼 {shares} 股會大於等於當時庫存 {current_shares} 股；全出請改 Close。"
                )
            inventory[stock_id] = current_shares - shares
        elif action == "Close":
            if current_shares <= 0:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} 前沒有庫存，不能使用 Close。"
                )
            if shares != current_shares:
                raise ValueError(
                    f"{stock_id} 在 {pd.to_datetime(row['date']).strftime('%Y-%m-%d')} Close 必須剛好賣出當時全部 {current_shares} 股。"
                )
            inventory[stock_id] = 0
        else:
            raise ValueError(f"不支援的交易動作：{action}")


def execute_trade(
    portfolio_id,
    date,
    stock_id,
    action,
    price,
    shares,
    is_disposed,
    basic,
    tech,
    chips,
    indus,
    notes,
    technical_score=3,
    chip_score=3,
    theme_score=3,
    take_profit_price=None,
    stop_loss_price=None,
):
    stock_id = normalize_stock_id(stock_id)
    date = pd.to_datetime(date).strftime("%Y-%m-%d")
    assert_trade_date_is_open(date)
    validate_trade_request(portfolio_id, stock_id, action, shares)
    candidate_trade = {
        "id": 10**15,
        "portfolio_id": portfolio_id,
        "date": date,
        "stock_id": stock_id,
        "action": action,
        "price": price,
        "shares": shares,
    }
    validate_trade_timeline(portfolio_id, candidate_trade=candidate_trade)

    is_settled = 1 if (is_disposed or action == "Setup") else 0
    technical_score = max(1, min(5, int(technical_score or 3)))
    chip_score = max(1, min(5, int(chip_score or 3)))
    theme_score = max(1, min(5, int(theme_score or 3)))
    take_profit_price = _normalize_optional_price_target(take_profit_price)
    stop_loss_price = _normalize_optional_price_target(stop_loss_price)
    stored_notes = _compose_trade_notes_with_risk(
        notes,
        take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price,
    )
    trade_repository.insert_trade(
        {
            "portfolio_id": portfolio_id,
            "date": date,
            "stock_id": stock_id,
            "action": action,
            "price": price,
            "shares": shares,
            "is_disposed": bool(is_disposed),
            "basic_check": bool(basic),
            "tech_check": bool(tech),
            "chips_check": bool(chips),
            "industry_check": bool(indus),
            "trading_notes": stored_notes,
            "settled": is_settled,
            "technical_score": technical_score,
            "chip_score": chip_score,
            "theme_score": theme_score,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
        }
    )
    recalculate_portfolio_cash(portfolio_id)
    mark_nav_snapshots_dirty(portfolio_id, date)


def update_trade_record(
    portfolio_id,
    trade_id,
    date,
    stock_id,
    action,
    price,
    shares,
    is_disposed,
    notes,
    technical_score=3,
    chip_score=3,
    theme_score=3,
    take_profit_price=None,
    stop_loss_price=None,
):
    trade_id = int(trade_id)
    stock_id = normalize_stock_id(stock_id)
    date = pd.to_datetime(date).strftime("%Y-%m-%d")
    action = str(action).strip()
    shares = int(shares or 0)
    price = float(price or 0)
    technical_score = max(1, min(5, int(technical_score or 3)))
    chip_score = max(1, min(5, int(chip_score or 3)))
    theme_score = max(1, min(5, int(theme_score or 3)))
    take_profit_price = _normalize_optional_price_target(take_profit_price)
    stop_loss_price = _normalize_optional_price_target(stop_loss_price)
    stored_notes = _compose_trade_notes_with_risk(
        notes,
        take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price,
    )
    assert_trade_date_is_open(date)

    existing_trade = trade_repository.get_trade_identity(portfolio_id, trade_id)

    if not existing_trade:
        raise ValueError("找不到這筆交易紀錄，可能已被刪除或資金池不一致。")

    is_settled = 1 if (is_disposed or action == "Setup") else 0
    validate_trade_timeline(
        portfolio_id,
        candidate_trade={
            "id": trade_id,
            "portfolio_id": portfolio_id,
            "date": date,
            "stock_id": stock_id,
            "action": action,
            "price": price,
            "shares": shares,
        },
        replaced_trade_id=trade_id,
    )

    trade_repository.update_trade(
        portfolio_id,
        trade_id,
        {
            "date": date,
            "stock_id": stock_id,
            "action": action,
            "price": price,
            "shares": shares,
            "is_disposed": bool(is_disposed),
            "trading_notes": stored_notes,
            "settled": is_settled,
            "technical_score": technical_score,
            "chip_score": chip_score,
            "theme_score": theme_score,
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
        },
    )

    recalculate_portfolio_cash(portfolio_id)
    old_date = str(existing_trade["date"] or date)
    mark_nav_snapshots_dirty(portfolio_id, min(old_date, date))


def update_holding_risk_targets(
    portfolio_id,
    stock_id,
    take_profit_price=None,
    stop_loss_price=None,
):
    stock_id = normalize_stock_id(stock_id)
    take_profit_price = _normalize_optional_price_target(take_profit_price)
    stop_loss_price = _normalize_optional_price_target(stop_loss_price)

    trades_df = trade_repository.list_trades(portfolio_id)
    if trades_df.empty:
        raise ValueError(f"{stock_id} 目前沒有可更新的持倉交易紀錄。")

    stock_trades_df = trades_df[
        trades_df["stock_id"].apply(normalize_stock_id) == stock_id
    ].copy()
    if stock_trades_df.empty:
        raise ValueError(f"{stock_id} 目前沒有可更新的持倉交易紀錄。")

    stock_trades_df["date"] = pd.to_datetime(stock_trades_df["date"], errors="coerce")
    stock_trades_df["id"] = pd.to_numeric(
        stock_trades_df["id"], errors="coerce"
    ).fillna(-1).astype(int)
    stock_trades_df = (
        stock_trades_df.dropna(subset=["date"])
        .sort_values(["date", "id"], kind="stable")
        .reset_index(drop=True)
    )

    current_shares = 0
    open_segment_rows = []
    for _, row in stock_trades_df.iterrows():
        action = str(row.get("action", "")).strip()
        shares = int(row.get("shares", 0) or 0)
        if action in ["Buy", "Setup"]:
            if current_shares <= 0:
                open_segment_rows = []
            current_shares += shares
            open_segment_rows.append(row)
        elif action == "Add":
            current_shares += shares
            open_segment_rows.append(row)
        elif action == "Reduce":
            current_shares = max(0, current_shares - shares)
            open_segment_rows.append(row)
        elif action == "Close":
            current_shares = max(0, current_shares - shares)
            open_segment_rows.append(row)
            if current_shares == 0:
                open_segment_rows = []

    if current_shares <= 0 or not open_segment_rows:
        raise ValueError(f"{stock_id} 目前沒有庫存，不能直接修改持倉停利/停損。")

    target_row = open_segment_rows[-1]
    target_trade_id = int(target_row.get("id"))
    stored_notes = _compose_trade_notes_with_risk(
        target_row.get("trading_notes", ""),
        take_profit_price=take_profit_price,
        stop_loss_price=stop_loss_price,
    )

    trade_repository.update_trade(
        portfolio_id,
        target_trade_id,
        {
            "take_profit_price": take_profit_price,
            "stop_loss_price": stop_loss_price,
            "trading_notes": stored_notes,
        },
    )


def delete_trade_record(portfolio_id, trade_id):
    trade_id = int(trade_id)

    existing_trade = trade_repository.get_trade_identity(portfolio_id, trade_id)

    if not existing_trade:
        raise ValueError("找不到這筆交易紀錄，可能已被刪除或資金池不一致。")

    validate_trade_timeline(portfolio_id, replaced_trade_id=trade_id)

    trade_repository.delete_trade(portfolio_id, trade_id)

    recalculate_portfolio_cash(portfolio_id)
    mark_nav_snapshots_dirty(portfolio_id, str(existing_trade["date"]))


def recalculate_portfolio_cash(portfolio_id):
    initial_cash = portfolio_repository.get_initial_cash(portfolio_id)
    cf_df = cashflow_repository.list_cashflows(portfolio_id, columns="type,amount")
    total_cf = sum(
        [
            float(row["amount"] or 0)
            if row["type"] != "Withdrawal"
            else -float(row["amount"] or 0)
            for _, row in cf_df.iterrows()
        ]
    )

    tr_all_df = trade_repository.list_trades(
        portfolio_id, columns="action,price,shares,settled"
    )
    t0_tr_flow = sum(
        [
            -float(row["price"] or 0) * float(row["shares"] or 0)
            if row["action"] in ["Buy", "Add"]
            else float(row["price"] or 0) * float(row["shares"] or 0)
            for _, row in tr_all_df.iterrows()
            if row["action"] != "Setup"
        ]
    )

    tr_settled_df = tr_all_df[
        pd.to_numeric(tr_all_df["settled"], errors="coerce").fillna(0).astype(int) == 1
    ]
    t2_tr_flow = sum(
        [
            -float(row["price"] or 0) * float(row["shares"] or 0)
            if row["action"] in ["Buy", "Add"]
            else float(row["price"] or 0) * float(row["shares"] or 0)
            for _, row in tr_settled_df.iterrows()
            if row["action"] != "Setup"
        ]
    )

    new_t0 = initial_cash + total_cf + t0_tr_flow
    new_t2 = initial_cash + total_cf + t2_tr_flow
    portfolio_repository.update_cash(portfolio_id, new_t0, new_t2)


def auto_process_settlement(portfolio_id):
    today = datetime.now().strftime("%Y-%m-%d")
    holiday_dates = get_settlement_blocked_dates()
    rows = trade_repository.list_unsettled_trades(portfolio_id, is_disposed=False)

    changed = 0
    for _, row in rows.iterrows():
        if (
            estimate_settlement_date(
                row["date"], holiday_dates=holiday_dates
            )
            <= today
        ):
            trade_repository.mark_trade_settled(row["id"])
            changed += 1

    recalculate_portfolio_cash(portfolio_id)


def execute_cashflow(portfolio_id, date, cf_type, amount):
    cashflow_repository.insert_cashflow(portfolio_id, date, cf_type, amount)
    recalculate_portfolio_cash(portfolio_id)
    mark_nav_snapshots_dirty(portfolio_id, date)


def get_macro_journals():
    return macro_journal_repository.list_journals()


def save_macro_journal(date, content):
    macro_journal_repository.upsert_journal(date, content)


def delete_macro_journal(date):
    macro_journal_repository.delete_journal(date)


def _normalize_notebook_update_log(update_log_json, edit_date=None):
    try:
        update_dates = json.loads(update_log_json or "[]")
    except Exception:
        update_dates = []

    if not isinstance(update_dates, list):
        update_dates = []

    normalized_dates = []
    for date_item in update_dates:
        date_text = str(date_item or "").strip()
        if date_text and date_text not in normalized_dates:
            normalized_dates.append(date_text)

    if edit_date:
        edit_date_text = str(edit_date).strip()
        if edit_date_text and edit_date_text not in normalized_dates:
            normalized_dates.append(edit_date_text)

    return json.dumps(normalized_dates, ensure_ascii=False)


def get_notebook_articles(note_type):
    return notebook_article_repository.list_articles(note_type)


def create_notebook_article(note_type, title, content, created_date):
    normalized_title = str(title or "").strip()
    normalized_content = str(content or "").strip()
    normalized_date = str(created_date or "").strip()
    if not normalized_title:
        raise ValueError("筆記標題不能空白。")
    if not normalized_content:
        raise ValueError("筆記內容不能空白。")
    if not normalized_date:
        raise ValueError("請選擇寫作日期。")

    update_log_json = _normalize_notebook_update_log("[]", normalized_date)
    return notebook_article_repository.create_article(
        str(note_type or "").strip(),
        normalized_title,
        normalized_content,
        normalized_date,
        update_log_json,
    )


def update_notebook_article(article_id, title, content, edit_date, update_log_json):
    normalized_title = str(title or "").strip()
    normalized_content = str(content or "").strip()
    if not normalized_title:
        raise ValueError("筆記標題不能空白。")
    if not normalized_content:
        raise ValueError("筆記內容不能空白。")

    normalized_update_log = _normalize_notebook_update_log(update_log_json, edit_date)
    notebook_article_repository.update_article(
        int(article_id),
        normalized_title,
        normalized_content,
        normalized_update_log,
    )


def delete_notebook_article(article_id):
    notebook_article_repository.delete_article(int(article_id))


def get_manual_price_overrides(stock_ids=None):
    normalized = [normalize_stock_id(s) for s in stock_ids] if stock_ids else None
    return manual_price_override_repository.get_overrides(normalized)


def set_manual_price_override(stock_id, price):
    stock_id = normalize_stock_id(stock_id)
    manual_price_override_repository.set_override(stock_id, price)


def delete_manual_price_override(stock_id):
    stock_id = normalize_stock_id(stock_id)
    manual_price_override_repository.delete_override(stock_id)


def _normalize_history_index(data_obj):
    if data_obj is None or data_obj.empty:
        return data_obj

    normalized = data_obj.copy()
    normalized.index = pd.to_datetime(normalized.index, errors="coerce")
    normalized = normalized[~normalized.index.isna()]
    if getattr(normalized.index, "tz", None) is not None:
        normalized.index = normalized.index.tz_localize(None)
    normalized.index = normalized.index.normalize()
    return normalized.sort_index()


def fetch_finmind_last_close(stock_id, lookback_days=45):
    sid = normalize_stock_id(stock_id)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": sid,
        "start_date": start_date,
        "end_date": end_date,
    }
    token = os.getenv("FINMIND_TOKEN", "").strip()
    if token:
        params["token"] = token

    try:
        r = _session.get(FINMIND_API_URL, params=params, timeout=6)
        if r.status_code != 200:
            return 0.0
        data = r.json()
        rows = data.get("data", [])
        if not rows:
            return 0.0
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        last_row = rows[-1]
        close_price = last_row.get("close", 0)
        return float(close_price) if close_price else 0.0
    except Exception:
        return 0.0


@st.cache_data(ttl=1800)
def fetch_finmind_price_history(stock_id, start_date=None, end_date=None):
    sid = normalize_stock_id(stock_id)
    end_dt = pd.to_datetime(end_date) if end_date else pd.to_datetime(datetime.now())
    start_dt = (
        pd.to_datetime(start_date)
        if start_date
        else end_dt - pd.Timedelta(days=30)
    )
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": sid,
        "start_date": start_date,
        "end_date": end_date,
    }
    token = os.getenv("FINMIND_TOKEN", "").strip()
    if token:
        params["token"] = token

    try:
        r = _session.get(FINMIND_API_URL, params=params, timeout=8)
        if r.status_code != 200:
            return pd.Series(dtype=float)
        data = r.json()
        rows = data.get("data", [])
        if not rows:
            return pd.Series(dtype=float)
        df = pd.DataFrame(rows)
        if "date" not in df.columns or "close" not in df.columns:
            return pd.Series(dtype=float)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["date", "close"]).sort_values("date")
        if df.empty:
            return pd.Series(dtype=float)
        return pd.Series(df["close"].values, index=df["date"], dtype=float)
    except Exception:
        return pd.Series(dtype=float)


def get_symbol_candidates(sid):
    sid = normalize_stock_id(sid)
    candidates = []
    cached_symbol = stock_name_repository.get_full_symbol(sid)
    if cached_symbol:
        candidates.append(cached_symbol)

    candidates.extend([f"{sid}.TW", f"{sid}.TWO"])
    return list(dict.fromkeys(candidates))


def update_stock_full_symbol(stock_id, full_symbol):
    stock_id = normalize_stock_id(stock_id)
    stock_name_repository.update_full_symbol(stock_id, full_symbol)


def fetch_first_valid_history(
    stock_id,
    start_date=None,
    period="1mo",
    auto_adjust=True,
    symbol_candidates=None,
    persist_symbol=True,
):
    end_d = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    candidates = symbol_candidates or get_symbol_candidates(stock_id)
    for candidate in candidates:
        try:
            t_obj = yf.Ticker(candidate)
            hist_kwargs = {
                "end": end_d,
                "auto_adjust": auto_adjust,
            }
            if start_date:
                hist_kwargs["start"] = start_date
            else:
                hist_kwargs["period"] = period if period else "1mo"
            hist = t_obj.history(**hist_kwargs)
            if not hist.empty:
                if persist_symbol:
                    update_stock_full_symbol(stock_id, candidate)
                return candidate, hist
        except Exception:
            continue

    return None, pd.DataFrame()


def get_full_yf_symbol(sid):
    sid = normalize_stock_id(sid)
    cached_symbol = stock_name_repository.get_full_symbol(sid)
    if cached_symbol:
        return cached_symbol

    try:
        url = f"https://tw.stock.yahoo.com/quote/{sid}"
        r = _session.get(url, timeout=3)
        if r.status_code == 200:
            return f"{sid}.TWO" if "上櫃" in r.text else f"{sid}.TW"
    except Exception:
        pass

    resolved_symbol, _ = fetch_first_valid_history(
        sid, period="5d", auto_adjust=True
    )
    if resolved_symbol:
        return resolved_symbol

    return sid + ".TW"


@st.cache_data(ttl=600)
def fetch_yfinance_history(tickers, start_date=None, period="1mo"):
    if not tickers:
        return pd.DataFrame(), pd.DataFrame(), {}

    results_close = {}
    results_divs = {}
    symbol_map = {}
    sids = list(dict.fromkeys(normalize_stock_id(t) for t in tickers))
    candidate_map = {sid: get_symbol_candidates(sid) for sid in sids}

    def get_one_ticker(stock_id):
        resolved_symbol, hist = fetch_first_valid_history(
            stock_id,
            start_date=start_date,
            period=period,
            auto_adjust=True,
            symbol_candidates=candidate_map.get(stock_id),
            persist_symbol=False,
        )
        if hist.empty:
            finmind_hist = fetch_finmind_price_history(stock_id, start_date=start_date)
            fallback_symbol = (
                (candidate_map.get(stock_id) or [f"{stock_id}.TW"])[0]
                if stock_id
                else None
            )
            if not finmind_hist.empty:
                return (
                    stock_id,
                    fallback_symbol,
                    finmind_hist.ffill(),
                    pd.Series(0.0, index=finmind_hist.index),
                    False,
                )
            return (
                stock_id,
                fallback_symbol,
                pd.Series(dtype=float),
                pd.Series(dtype=float),
                False,
            )

        close_series = hist["Close"].ffill()
        div_series = (
            hist["Dividends"]
            if "Dividends" in hist
            else pd.Series(0.0, index=hist.index)
        )
        return stock_id, resolved_symbol, close_series, div_series, True

    with ThreadPoolExecutor(max_workers=5) as executor:
        merged_res = list(executor.map(get_one_ticker, sids))

    for sid, resolved_symbol, close_series, div_series, should_persist_symbol in merged_res:
        symbol_map[sid] = resolved_symbol
        results_close[sid] = _normalize_history_index(close_series)
        results_divs[sid] = _normalize_history_index(div_series)
        if should_persist_symbol and resolved_symbol and not close_series.empty:
            update_stock_full_symbol(sid, resolved_symbol)

    finmind_bm = fetch_finmind_price_history("0050", start_date=start_date)
    if not finmind_bm.empty:
        finmind_bm = _normalize_history_index(finmind_bm)
        results_close["0050.TW"] = finmind_bm.ffill()
        results_divs["0050.TW"] = pd.Series(0.0, index=finmind_bm.index)
        symbol_map["0050"] = "0050.TW"
    else:
        benchmark_symbol, benchmark_hist = fetch_first_valid_history(
            "0050", start_date=start_date, period=period, auto_adjust=True
        )
        if not benchmark_hist.empty:
            results_close["0050.TW"] = _normalize_history_index(
                benchmark_hist["Close"]
            ).ffill()
            results_divs["0050.TW"] = (
                _normalize_history_index(benchmark_hist["Dividends"])
                if "Dividends" in benchmark_hist
                else pd.Series(
                    0.0,
                    index=_normalize_history_index(benchmark_hist["Close"]).index,
                )
            )
            if benchmark_symbol:
                symbol_map["0050"] = benchmark_symbol
        elif "0050.TW" not in results_close:
            results_close["0050.TW"] = pd.Series(dtype=float)
            results_divs["0050.TW"] = pd.Series(dtype=float)

    symbol_map.setdefault("0050", "0050.TW")

    close_df = _normalize_history_index(pd.DataFrame(results_close)).ffill()
    divs_df = _normalize_history_index(pd.DataFrame(results_divs)).fillna(0.0)
    return close_df, divs_df, symbol_map


def upsert_price_snapshots(price_df, source="auto_history"):
    if price_df.empty:
        return 0

    records_map = {}
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    normalized_df = price_df.copy()
    normalized_df.index = pd.to_datetime(normalized_df.index, errors="coerce")
    normalized_df = normalized_df[~normalized_df.index.isna()]

    for col in normalized_df.columns:
        sid = normalize_stock_id(col)
        series = pd.to_numeric(normalized_df[col], errors="coerce").dropna()
        if series.empty:
            continue
        for dt, close_price in series.items():
            snapshot_date = pd.to_datetime(dt).strftime("%Y-%m-%d")
            records_map[(snapshot_date, sid)] = {
                "date": snapshot_date,
                "stock_id": sid,
                "close_price": float(close_price),
                "source": str(source),
                "updated_at": now_str,
            }
    return price_snapshot_repository.upsert_snapshots(list(records_map.values()))


def get_price_snapshot_history(stock_ids, start_date=None, end_date=None):
    normalized_ids = [normalize_stock_id(s) for s in stock_ids if str(s).strip()]
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return pd.DataFrame()

    snapshot_rows = price_snapshot_repository.get_snapshot_rows(
        normalized_ids,
        pd.to_datetime(start_date).strftime("%Y-%m-%d") if start_date else None,
        pd.to_datetime(end_date).strftime("%Y-%m-%d") if end_date else None,
    )
    df = pd.DataFrame(snapshot_rows or [])

    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df = df.dropna(subset=["date", "close_price"])
    if df.empty:
        return pd.DataFrame()
    snapshot_df = df.pivot(
        index="date", columns="stock_id", values="close_price"
    ).sort_index()
    return _normalize_history_index(snapshot_df)


def refresh_price_snapshots_for_tickers(stock_ids, start_date, end_date=None):
    normalized_ids = [normalize_stock_id(s) for s in stock_ids if str(s).strip()]
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return {"written": 0, "tickers": 0}

    price_df, _, _ = fetch_yfinance_history(normalized_ids, start_date=start_date, period=None)
    written = upsert_price_snapshots(price_df, source="auto_history")
    return {"written": written, "tickers": len(normalized_ids)}


def refresh_portfolio_price_snapshots(portfolio_id):
    trades_df = trade_repository.list_trades(portfolio_id, columns="date,stock_id")

    if trades_df.empty:
        return {"written": 0, "tickers": 0, "start_date": None}

    trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)
    start_date = trades_df["date"].min()
    tickers = trades_df["stock_id"].dropna().unique().tolist()
    result = refresh_price_snapshots_for_tickers(tickers + ["0050"], start_date=start_date)
    result["start_date"] = start_date
    return result


def get_portfolio_snapshot_status(portfolio_id):
    trades_df = trade_repository.list_trades(portfolio_id, columns="stock_id")
    if trades_df.empty:
        return {"tickers": 0, "dates": 0, "latest_date": None}
    stock_ids = trades_df["stock_id"].apply(normalize_stock_id).tolist()
    stock_ids = list(dict.fromkeys(stock_ids + ["0050"]))
    status = price_snapshot_repository.get_snapshot_status(stock_ids)
    return {
        "tickers": len(stock_ids),
        "dates": status["dates"],
        "latest_date": status["latest_date"],
    }


def get_price_snapshot_min_latest_date(stock_ids):
    normalized_ids = [normalize_stock_id(s) for s in stock_ids if str(s).strip()]
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        return None

    return price_snapshot_repository.get_min_latest_date(normalized_ids)


def get_daily_nav_snapshots_df(portfolio_id):
    df = daily_nav_snapshot_repository.list_snapshots(portfolio_id)
    if not df.empty:
        df = df.rename(
            columns={
                "date": "Date",
                "nav": "NAV",
                "cash_flow": "CashFlow",
                "cash": "Cash",
                "holdings_value": "Holdings",
                "daily_return": "DailyReturn",
                "twr": "TWR",
                "benchmark_price": "BenchmarkPrice",
                "notes": "Notes",
                "is_trading_day": "IsTradingDay",
                "benchmark_daily_return": "BenchmarkDailyReturn",
                "benchmark_cum_return": "BenchmarkCumReturn",
                "drawdown": "Drawdown",
                "daily_alpha": "DailyAlpha",
                "cum_alpha": "CumAlpha",
                "allocation_json": "AllocationJson",
                "win_flag": "Win?",
            }
        )

    if df.empty:
        return pd.DataFrame()

    numeric_cols = [
        "NAV",
        "CashFlow",
        "Cash",
        "Holdings",
        "DailyReturn",
        "TWR",
        "BenchmarkPrice",
        "BenchmarkDailyReturn",
        "BenchmarkCumReturn",
        "Drawdown",
        "DailyAlpha",
        "CumAlpha",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["IsTradingDay"] = df["IsTradingDay"].fillna(0).astype(bool)
    df["Notes"] = df["Notes"].fillna("")
    df["AllocationJson"] = df["AllocationJson"].fillna("{}")
    df["Win?"] = df["Win?"].fillna("-")
    return df


def _compute_holdings_market_value_on_date(trades_df, price_data, target_date):
    if trades_df is None or trades_df.empty or price_data is None or price_data.empty:
        return 0.0

    target_dt = pd.to_datetime(target_date, errors="coerce")
    if pd.isna(target_dt):
        return 0.0
    target_str = target_dt.strftime("%Y-%m-%d")

    working_df = trades_df.copy()
    working_df["date"] = pd.to_datetime(working_df["date"], errors="coerce")
    working_df = working_df.dropna(subset=["date"])
    working_df = working_df[working_df["date"] <= target_dt].copy()
    if working_df.empty:
        return 0.0

    working_df["stock_id"] = working_df["stock_id"].apply(normalize_stock_id)
    working_df["shares"] = pd.to_numeric(working_df["shares"], errors="coerce").fillna(0).astype(int)
    inventory = {}
    for _, row in working_df.iterrows():
        sid = row["stock_id"]
        action = str(row.get("action", "") or "")
        inventory.setdefault(sid, 0)
        if action in ["Buy", "Add", "Setup"]:
            inventory[sid] += int(row["shares"])
        elif action in ["Reduce", "Close"]:
            inventory[sid] -= int(row["shares"])

    holdings_value = 0.0
    for sid, shares in inventory.items():
        if shares <= 0:
            continue
        close_price = _resolve_stock_price_on_date(
            price_data,
            sid,
            target_str,
            fallback_price=0.0,
        )
        holdings_value += float(close_price or 0) * float(shares)
    return float(holdings_value)


def _latest_nav_snapshot_out_of_sync(existing_nav_df, trades_df, latest_trading_date, price_data):
    if existing_nav_df is None or existing_nav_df.empty:
        return False

    latest_snapshot_df = existing_nav_df[
        existing_nav_df["Date"].astype(str) == str(latest_trading_date)
    ].copy()
    if latest_snapshot_df.empty:
        return False

    latest_row = latest_snapshot_df.iloc[-1]
    latest_benchmark_price = _resolve_stock_price_on_date(
        price_data,
        "0050",
        latest_trading_date,
        fallback_price=0.0,
    )
    snapshot_benchmark_price = float(latest_row.get("BenchmarkPrice", 0) or 0)
    if latest_benchmark_price > 0 and abs(snapshot_benchmark_price - latest_benchmark_price) > 1e-6:
        return True

    latest_holdings_value = _compute_holdings_market_value_on_date(
        trades_df,
        price_data,
        latest_trading_date,
    )
    snapshot_holdings_value = float(latest_row.get("Holdings", 0) or 0)
    if abs(snapshot_holdings_value - latest_holdings_value) > 1e-6:
        return True

    return False


def upsert_daily_nav_snapshots(portfolio_id, hist_df):
    if hist_df is None or hist_df.empty:
        return 0

    required_cols = [
        "Date",
        "NAV",
        "CashFlow",
        "Cash",
        "Holdings",
        "DailyReturn",
        "TWR",
        "BenchmarkPrice",
        "BenchmarkDailyReturn",
        "BenchmarkCumReturn",
        "Drawdown",
        "DailyAlpha",
        "CumAlpha",
        "Notes",
        "AllocationJson",
        "IsTradingDay",
        "Win?",
    ]
    df = hist_df.copy()
    for col in required_cols:
        if col not in df.columns:
            df[col] = "{}" if col == "AllocationJson" else ("" if col in ["Notes", "Win?"] else 0)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    records_map = {}
    for _, row in df.iterrows():
        dt = pd.to_datetime(row["Date"], errors="coerce")
        if pd.isna(dt):
            continue
        snapshot_date = dt.strftime("%Y-%m-%d")
        records_map[(portfolio_id, snapshot_date)] = (
            portfolio_id,
            snapshot_date,
            float(row["NAV"] or 0),
            float(row["Cash"] or 0),
            float(row["Holdings"] or 0),
            float(row["CashFlow"] or 0),
            float(row["DailyReturn"] or 0),
            float(row["TWR"] or 0),
            float(row["BenchmarkPrice"] or 0),
            float(row["BenchmarkDailyReturn"] or 0),
            float(row["BenchmarkCumReturn"] or 0),
            float(row["DailyAlpha"] or 0),
            float(row["CumAlpha"] or 0),
            float(row["Drawdown"] or 0),
            str(row["Notes"] or ""),
            str(row["AllocationJson"] or "{}"),
            int(bool(row["IsTradingDay"])),
            str(row["Win?"] or "-"),
            now_str,
        )

    records = [
        {
            "date": values[1],
            "nav": values[2],
            "cash": values[3],
            "holdings_value": values[4],
            "cash_flow": values[5],
            "daily_return": values[6],
            "twr": values[7],
            "benchmark_price": values[8],
            "benchmark_daily_return": values[9],
            "benchmark_cum_return": values[10],
            "daily_alpha": values[11],
            "cum_alpha": values[12],
            "drawdown": values[13],
            "notes": values[14],
            "allocation_json": values[15],
            "is_trading_day": values[16],
            "win_flag": values[17],
            "updated_at": values[18],
        }
        for values in records_map.values()
    ]
    written = daily_nav_snapshot_repository.upsert_snapshots(portfolio_id, records)
    if written:
        portfolio_repository.set_nav_dirty_from_date(portfolio_id, None)
    return written


@st.cache_data(ttl=86400)
def get_stock_display_names(tickers):
    if not tickers:
        return {}

    existing_map = {}
    existing_full = {}
    try:
        sids = [normalize_stock_id(t) for t in tickers]
        rows = stock_name_repository.list_stock_names(sids)
        for row in rows:
            existing_map[row["stock_id"]] = row["name"]
            existing_full[row["stock_id"]] = row["full_symbol"]
    except Exception:
        pass

    res = {}
    to_scrape = []
    for t in tickers:
        sid = normalize_stock_id(t)
        if sid in existing_map and existing_full.get(sid):
            res[t] = f"{sid} {existing_map[sid]}"
        else:
            to_scrape.append(t)

    if not to_scrape:
        return res

    def scrape_one(t_in):
        code = normalize_stock_id(t_in)
        name, full = None, None
        try:
            url = f"https://tw.stock.yahoo.com/quote/{code}"
            r = _session.get(url, timeout=3)
            if r.status_code == 200:
                html = r.text
                match = re.search(r"<title>([^<]+?)\s*\(", html)
                if match:
                    name = match.group(1).strip()
                full = f"{code}.TWO" if "上櫃" in html else f"{code}.TW"
        except Exception:
            pass

        if not name:
            try:
                for candidate in get_symbol_candidates(code):
                    tkr = yf.Ticker(candidate)
                    name = tkr.info.get("shortName")
                    if name:
                        full = candidate
                        break
            except Exception:
                pass
        return t_in, code, name, full

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(scrape_one, to_scrape))

    for t_in, code, name, full in results:
        if name:
            res[t_in] = f"{code} {name}"
            try:
                stock_name_repository.upsert_stock_name(code, name, full)
            except Exception:
                pass
        else:
            res[t_in] = t_in
    return res


def calculate_twr_and_nav(portfolio_id):
    ensure_db_schema()
    initial_cash = portfolio_repository.get_initial_cash(portfolio_id)
    dirty_from_date = portfolio_repository.get_nav_dirty_from_date(portfolio_id)
    trades_df = trade_repository.list_trades(portfolio_id)
    cf_df = cashflow_repository.list_cashflows(portfolio_id)

    if not trades_df.empty:
        trades_df = trades_df.copy()
        trades_df["date"] = pd.to_datetime(trades_df["date"], errors="coerce")
        trades_df = trades_df.dropna(subset=["date"]).sort_values(
            ["date", "id"] if "id" in trades_df.columns else ["date"],
            kind="stable",
        )
        trades_df["date"] = trades_df["date"].dt.strftime("%Y-%m-%d")
        trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)
        trades_df = trades_df.reset_index(drop=True)

    if not cf_df.empty:
        cf_df = cf_df.copy()
        cf_df["date"] = pd.to_datetime(cf_df["date"], errors="coerce")
        cf_df = cf_df.dropna(subset=["date"]).sort_values(
            ["date", "id"] if "id" in cf_df.columns else ["date"],
            kind="stable",
        )
        cf_df["date"] = cf_df["date"].dt.strftime("%Y-%m-%d")
        cf_df = cf_df.reset_index(drop=True)

    dates = []
    if not trades_df.empty:
        dates.extend(trades_df["date"].tolist())
    if not cf_df.empty:
        dates.extend(cf_df["date"].tolist())
    if not dates:
        if abs(initial_cash) <= 1e-9:
            return pd.DataFrame(), 0, 0
        dates.append(get_tw_now().strftime("%Y-%m-%d"))

    start_date_str = min(dates)
    if abs(initial_cash) > 1e-9:
        initial_cf_row = pd.DataFrame(
            [
                {
                    "portfolio_id": portfolio_id,
                    "date": start_date_str,
                    "type": "Deposit" if initial_cash >= 0 else "Withdrawal",
                    "amount": abs(initial_cash),
                }
            ]
        )
        cf_df = (
            initial_cf_row
            if cf_df.empty
            else pd.concat([initial_cf_row, cf_df], ignore_index=True)
        )
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    holiday_dates = get_market_holiday_dates()
    official_now = get_tw_now()
    latest_trading_date = get_latest_official_tw_trading_date(official_now, holiday_dates)
    end_date = datetime.strptime(latest_trading_date, "%Y-%m-%d")
    date_range = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date)]
    all_tickers = trades_df["stock_id"].unique().tolist() if not trades_df.empty else []

    existing_nav_df = get_daily_nav_snapshots_df(portfolio_id)
    stale_snapshot_date = _find_stale_allocation_snapshot_date(existing_nav_df, all_tickers)
    if stale_snapshot_date:
        mark_nav_snapshots_dirty(portfolio_id, stale_snapshot_date)
        dirty_from_date = portfolio_repository.get_nav_dirty_from_date(portfolio_id)
        existing_nav_df = get_daily_nav_snapshots_df(portfolio_id)
    snapshot_tickers = list(dict.fromkeys(all_tickers + ["0050"]))
    price_refresh_start = dirty_from_date or start_date_str
    min_price_snapshot_date = get_price_snapshot_min_latest_date(snapshot_tickers)
    if dirty_from_date is None and min_price_snapshot_date:
        if min_price_snapshot_date < latest_trading_date:
            price_refresh_start = min_price_snapshot_date
        else:
            price_refresh_start = None
    if price_refresh_start:
        refresh_price_snapshots_for_tickers(
            snapshot_tickers,
            start_date=price_refresh_start,
            end_date=latest_trading_date,
        )

    price_data = get_price_snapshot_history(
        snapshot_tickers,
        start_date=start_date_str,
        end_date=latest_trading_date,
    )
    if not price_data.empty:
        price_data = _normalize_history_index(price_data).ffill()
    _, divs_data, symbol_map = fetch_yfinance_history(all_tickers, start_date_str)
    if not divs_data.empty:
        divs_data = _normalize_history_index(divs_data).fillna(0.0)

    if not price_data.empty and "0050" not in price_data.columns and "0050.TW" in price_data.columns:
        price_data["0050"] = price_data["0050.TW"]
    symbol_map.setdefault("0050", "0050.TW")
    latest_trading_ts = pd.to_datetime(latest_trading_date)

    if (
        dirty_from_date is None
        and not existing_nav_df.empty
        and str(existing_nav_df.iloc[-1]["Date"]) >= latest_trading_date
    ):
        if _latest_nav_snapshot_out_of_sync(
            existing_nav_df,
            trades_df,
            latest_trading_date,
            price_data,
        ):
            mark_nav_snapshots_dirty(portfolio_id, latest_trading_date)
            dirty_from_date = portfolio_repository.get_nav_dirty_from_date(portfolio_id)
            existing_nav_df = get_daily_nav_snapshots_df(portfolio_id)
        else:
            return (
                existing_nav_df,
                existing_nav_df.iloc[-1]["NAV"],
                existing_nav_df.iloc[-1]["TWR"],
            )

    if all_tickers:
        manual_overrides = get_manual_price_overrides(all_tickers)
        if price_data.empty:
            price_data = pd.DataFrame(index=pd.DatetimeIndex([latest_trading_ts]))
        if latest_trading_ts not in price_data.index:
            price_data.loc[latest_trading_ts] = np.nan
        for sid in all_tickers:
            latest_price = manual_overrides.get(sid, np.nan)
            if pd.isna(latest_price) or latest_price <= 0:
                latest_price = fetch_finmind_last_close(sid)
            if pd.notna(latest_price) and float(latest_price) > 0:
                price_data.loc[latest_trading_ts, sid] = float(latest_price)

    latest_benchmark_price = fetch_finmind_last_close("0050")
    if latest_benchmark_price > 0:
        if price_data.empty:
            price_data = pd.DataFrame(index=pd.DatetimeIndex([latest_trading_ts]))
        if latest_trading_ts not in price_data.index:
            price_data.loc[latest_trading_ts] = np.nan
        price_data.loc[latest_trading_ts, "0050"] = float(latest_benchmark_price)
        price_data.loc[latest_trading_ts, "0050.TW"] = float(latest_benchmark_price)

    if not price_data.empty:
        price_data = _normalize_history_index(price_data).sort_index().ffill()

    trading_day_index = (
        set(price_data.index.strftime("%Y-%m-%d")) if not price_data.empty else set()
    )

    history = []
    events_by_date = {d: {"trades": [], "cf": []} for d in date_range}

    for _, r in trades_df.iterrows():
        event_date = str(r["date"])
        if event_date in events_by_date:
            events_by_date[event_date]["trades"].append(r)
    for _, r in cf_df.iterrows():
        event_date = str(r["date"])
        if event_date in events_by_date:
            events_by_date[event_date]["cf"].append(r)

    trade_price_fallback = {}
    if not trades_df.empty:
        for stock_id, group in trades_df.groupby("stock_id"):
            trade_price_fallback[stock_id] = group[["date", "price"]].sort_values("date")

    current_cash = 0.0
    current_inv = {}
    last_nav = 0.0
    cum_twr = 1.0

    last_b_price = 100.0

    for d in date_range:
        daily_ext_cf = 0.0
        has_events = False

        for cf in events_by_date[d]["cf"]:
            has_events = True
            if cf["type"] == "Withdrawal":
                current_cash -= cf["amount"]
                daily_ext_cf -= cf["amount"]
            elif cf["type"] == "Deposit":
                current_cash += cf["amount"]
                daily_ext_cf += cf["amount"]
            elif cf["type"] == "Dividend":
                current_cash += cf["amount"]

        for tr in events_by_date[d]["trades"]:
            has_events = True
            s_id = tr["stock_id"]
            shares = tr["shares"]
            val = tr["price"] * shares
            if tr["action"] in ["Buy", "Add", "Setup"]:
                if tr["action"] != "Setup":
                    current_cash -= val
                current_inv[s_id] = current_inv.get(s_id, 0) + shares
            else:
                current_cash += val
                current_inv[s_id] = current_inv.get(s_id, 0) - shares

        inv_value = 0.0
        position_values = {}
        for s_id, sh in current_inv.items():
            if sh <= 0:
                continue

            yf_code = symbol_map.get(s_id)
            price_col = s_id if s_id in price_data.columns else yf_code
            div_col = s_id if s_id in divs_data.columns else yf_code

            if not divs_data.empty and div_col in divs_data.columns:
                try:
                    div_series = divs_data[div_col].copy()
                    div_series = _normalize_history_index(div_series)
                    div_on_day = div_series.loc[div_series.index == pd.to_datetime(d)]
                    if not div_on_day.empty:
                        div_v = div_on_day.iloc[-1]
                        if pd.notna(div_v) and float(div_v) > 0:
                            current_cash += float(div_v) * sh
                except Exception:
                    pass

            price = np.nan
            if not price_data.empty and price_col in price_data.columns:
                try:
                    price_series = price_data[price_col].copy()
                    price_series = _normalize_history_index(
                        price_series
                    ).dropna()
                    current_dt = pd.to_datetime(d)
                    past_prices = price_series.loc[price_series.index <= current_dt]
                    if not past_prices.empty:
                        price = past_prices.iloc[-1]
                    else:
                        future_prices = price_series.loc[price_series.index >= current_dt]
                        if not future_prices.empty:
                            price = future_prices.iloc[0]
                except Exception:
                    price = np.nan

            if pd.isna(price) and s_id in trade_price_fallback:
                fallback_df = trade_price_fallback[s_id]
                past_prices = fallback_df[fallback_df["date"] <= d]
                if not past_prices.empty:
                    price = float(past_prices.iloc[-1]["price"])

            if not pd.isna(price):
                pos_value = float(price) * sh
                inv_value += pos_value
                position_values[s_id] = round(pos_value, 4)

        daily_nav = current_cash + inv_value
        allocation_payload = {
            "可用現金 (T+0)": {
                "value": round(float(current_cash), 4),
                "weight_pct": (
                    round(float(current_cash) / daily_nav * 100, 4)
                    if abs(daily_nav) > 1e-9
                    else 0.0
                ),
            }
        }
        for alloc_sid, alloc_value in sorted(
            position_values.items(), key=lambda item: item[1], reverse=True
        ):
            allocation_payload[alloc_sid] = {
                "value": alloc_value,
                "weight_pct": (
                    round(float(alloc_value) / daily_nav * 100, 4)
                    if abs(daily_nav) > 1e-9
                    else 0.0
                ),
            }

        daily_return = 0.0
        if last_nav > 0:
            daily_return = (daily_nav - daily_ext_cf - last_nav) / last_nav

        cum_twr *= 1 + daily_return
        last_nav = daily_nav

        b_price = last_b_price
        benchmark_col = (
            "0050"
            if not price_data.empty and "0050" in price_data.columns
            else "0050.TW"
        )
        if not price_data.empty and benchmark_col in price_data.columns:
            try:
                benchmark_series = price_data[benchmark_col].copy()
                benchmark_series = _normalize_history_index(
                    benchmark_series
                ).dropna()
                current_dt = pd.to_datetime(d)
                past_bm = benchmark_series.loc[benchmark_series.index <= current_dt]
                if not past_bm.empty:
                    b_price = past_bm.iloc[-1]
                else:
                    future_bm = benchmark_series.loc[benchmark_series.index >= current_dt]
                    if not future_bm.empty:
                        b_price = future_bm.iloc[0]
            except Exception:
                b_price = last_b_price
        last_b_price = float(b_price)

        is_trading_day = is_tw_market_open(d, holiday_dates)

        if is_trading_day or has_events or d == start_date_str:
            trade_summary = ", ".join(
                [
                    f"{tr['action']} {tr['stock_id']} {tr['shares']}股"
                    for tr in events_by_date[d]["trades"]
                ]
            )
            history.append(
                {
                    "Date": d,
                    "NAV": daily_nav,
                    "CashFlow": daily_ext_cf,
                    "Cash": current_cash,
                    "Holdings": inv_value,
                    "DailyReturn": daily_return * 100,
                    "TWR": (cum_twr - 1.0) * 100,
                    "BenchmarkPrice": float(b_price),
                    "Notes": trade_summary,
                    "AllocationJson": json.dumps(
                        allocation_payload, ensure_ascii=False
                    ),
                    "IsTradingDay": is_trading_day,
                }
            )

    df_hist = pd.DataFrame(history)
    if not df_hist.empty:
        df_hist["BenchmarkDailyReturn"] = (
            df_hist["BenchmarkPrice"].pct_change().fillna(0) * 100
        )
        base_0050 = df_hist.iloc[0]["BenchmarkPrice"]
        if base_0050 > 0:
            df_hist["BenchmarkCumReturn"] = (
                df_hist["BenchmarkPrice"] / base_0050 - 1.0
            ) * 100
        else:
            df_hist["BenchmarkCumReturn"] = 0.0

        twr_index = df_hist["TWR"] / 100.0 + 1.0
        rolling_max = twr_index.cummax()
        df_hist["Drawdown"] = np.where(
            rolling_max > 0, (twr_index - rolling_max) / rolling_max * 100, 0.0
        )
        df_hist["DailyAlpha"] = df_hist["DailyReturn"] - df_hist["BenchmarkDailyReturn"]
        df_hist["CumAlpha"] = df_hist["TWR"] - df_hist["BenchmarkCumReturn"]
        df_hist["Win?"] = df_hist["DailyAlpha"].apply(
            lambda x: "Y" if x > 0 else ("N" if x < 0 else "-")
        )

    return (
        get_daily_nav_snapshots_df(portfolio_id)
        if upsert_daily_nav_snapshots(portfolio_id, df_hist)
        else df_hist,
        df_hist.iloc[-1]["NAV"] if not df_hist.empty else 0,
        df_hist.iloc[-1]["TWR"] if not df_hist.empty else (cum_twr - 1.0) * 100,
    )


@st.cache_data(ttl=86400)
def get_stock_dividends(stock_id, start_date):
    yf_code = get_full_yf_symbol(stock_id)
    try:
        end_date_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        data = yf.download(
            yf_code,
            start=start_date,
            end=end_date_str,
            auto_adjust=False,
            actions=True,
            progress=False,
        )
        if "Dividends" in data:
            d = data["Dividends"]
            if isinstance(d, pd.DataFrame):
                d = d.iloc[:, 0]
            return d[d > 0].dropna()
    except Exception:
        pass
    return pd.Series(dtype=float)


def calculate_trade_journal(trades_df):
    if trades_df.empty:
        return pd.DataFrame()

    s_id = trades_df.iloc[0]["stock_id"]
    from_date = trades_df["date"].min()
    divs = get_stock_dividends(s_id, from_date)
    trades_df = trades_df.copy()
    trades_df["date"] = pd.to_datetime(trades_df["date"], errors="coerce")
    if "id" in trades_df.columns:
        trades_df["id"] = pd.to_numeric(trades_df["id"], errors="coerce").fillna(0)
    else:
        trades_df["id"] = 0
    trades_df = trades_df.dropna(subset=["date"]).sort_values(
        ["date", "id"], kind="stable"
    )
    trades_df["date"] = trades_df["date"].dt.strftime("%Y-%m-%d")

    if not divs.empty:
        div_rows = []
        for d, amt in divs.items():
            if d >= pd.to_datetime(from_date):
                div_rows.append(
                    {
                        "id": -1,
                        "portfolio_id": trades_df.iloc[0]["portfolio_id"],
                        "date": d.strftime("%Y-%m-%d"),
                        "stock_id": s_id,
                        "action": "Dividend",
                        "price": float(amt),
                        "shares": 0,
                        "is_disposed": 0,
                        "disposed_cost": 0,
                        "disposed_tax": 0,
                        "trading_notes": f"系統自動除權息: ${float(amt):.2f}/股",
                    }
                )
        if div_rows:
            trades_df = (
                pd.concat([trades_df, pd.DataFrame(div_rows)], ignore_index=True)
                .assign(date=lambda df: pd.to_datetime(df["date"], errors="coerce"))
                .dropna(subset=["date"])
                .sort_values(["date", "id"], kind="stable")
                .assign(date=lambda df: df["date"].dt.strftime("%Y-%m-%d"))
                .reset_index(drop=True)
            )

    records = []
    avg_cost = 0.0
    accum_shares = 0
    cum_pnl = 0.0

    for _, row in trades_df.iterrows():
        action = row["action"]
        shares = row["shares"]
        price = row["price"]
        pnl_this_trade = 0.0
        avg_cost_before = avg_cost
        shares_before = accum_shares

        if action == "Dividend":
            if accum_shares > 0:
                pnl_this_trade = price * accum_shares
                cum_pnl += pnl_this_trade
                avg_cost = max(0.0, avg_cost - price)
            row["shares"] = accum_shares
        elif action in ["Buy", "Add", "Setup"]:
            total_cost = (avg_cost * accum_shares) + (price * shares)
            accum_shares += shares
            avg_cost = total_cost / accum_shares if accum_shares > 0 else 0
        elif action in ["Reduce", "Close"]:
            pnl_this_trade = (price - avg_cost) * shares
            cum_pnl += pnl_this_trade
            accum_shares -= shares
            if accum_shares <= 0:
                avg_cost = 0.0
                accum_shares = 0

        r_dict = dict(row)
        r_dict["avg_cost_before"] = avg_cost_before
        r_dict["avg_cost_after"] = avg_cost
        r_dict["shares_before"] = shares_before
        r_dict["avg_cost"] = avg_cost_before if action in ["Reduce", "Close", "Dividend"] else avg_cost
        r_dict["accum_shares"] = accum_shares
        r_dict["realized_pnl"] = pnl_this_trade
        r_dict["cum_pnl"] = cum_pnl
        records.append(r_dict)

    return pd.DataFrame(records)


def _resolve_stock_price_on_date(price_data, stock_id, lookup_date, fallback_price=np.nan):
    if price_data is None or price_data.empty:
        return fallback_price

    price_col = stock_id if stock_id in price_data.columns else None
    if price_col is None:
        full_symbol = get_full_yf_symbol(stock_id)
        if full_symbol in price_data.columns:
            price_col = full_symbol
    if price_col is None:
        return fallback_price

    try:
        price_series = _normalize_history_index(price_data[price_col].copy()).dropna()
        current_dt = pd.to_datetime(lookup_date, errors="coerce")
        if pd.isna(current_dt) or price_series.empty:
            return fallback_price
        past_prices = price_series.loc[price_series.index <= current_dt]
        if not past_prices.empty:
            return float(past_prices.iloc[-1])
        future_prices = price_series.loc[price_series.index >= current_dt]
        if not future_prices.empty:
            return float(future_prices.iloc[0])
    except Exception:
        return fallback_price
    return fallback_price


def _build_allocation_weight_map(portfolio_hist_df, stock_id):
    if portfolio_hist_df is None or portfolio_hist_df.empty:
        return {}

    weight_map = {}
    hist_copy = portfolio_hist_df.copy()
    hist_copy["Date"] = pd.to_datetime(hist_copy["Date"], errors="coerce")
    hist_copy = hist_copy.dropna(subset=["Date"]).sort_values("Date", kind="stable")
    for _, row in hist_copy.iterrows():
        date_key = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")
        try:
            allocation_obj = json.loads(row.get("AllocationJson", "{}") or "{}")
        except Exception:
            allocation_obj = {}
        weight_map[date_key] = float(
            (allocation_obj.get(stock_id) or {}).get("weight_pct", 0) or 0
        )
    return weight_map


def _build_trade_cycle_evaluation(cycle_summary):
    take_profit_target = cycle_summary.get("停利目標價")
    stop_loss_target = cycle_summary.get("停損目標價")
    close_price = float(cycle_summary.get("出場均價", 0) or 0)
    total_return_pct = float(cycle_summary.get("結算報酬率 (%)", 0) or 0)
    min_floating_pct = float(cycle_summary.get("最大浮虧率 (%)", 0) or 0)
    max_floating_pct = float(cycle_summary.get("最大浮盈率 (%)", 0) or 0)
    avg_score = float(cycle_summary.get("平均操作評分", 0) or 0)
    notes_fill_ratio = float(cycle_summary.get("心得填寫率 (%)", 0) or 0)

    discipline_parts = []
    if pd.notna(take_profit_target) and float(take_profit_target) > 0:
        if close_price >= float(take_profit_target):
            discipline_parts.append("有依停利價附近出場")
        elif max_floating_pct > total_return_pct + 5:
            discipline_parts.append("曾有較大浮盈回吐，可再檢討停利執行")
    else:
        discipline_parts.append("未設定停利目標")

    if pd.notna(stop_loss_target) and float(stop_loss_target) > 0:
        if close_price <= float(stop_loss_target) and total_return_pct < 0:
            discipline_parts.append("有執行停損出場")
        elif min_floating_pct <= -8 and total_return_pct < 0:
            discipline_parts.append("曾出現較大浮虧，需回看停損紀律")
    else:
        discipline_parts.append("未設定停損目標")

    quality_parts = []
    if avg_score >= 4:
        quality_parts.append("交易前評分偏高")
    elif avg_score > 0:
        quality_parts.append("交易前評分普通，適合搭配更明確進出場條件")
    if notes_fill_ratio >= 80:
        quality_parts.append("心得紀錄完整")
    elif notes_fill_ratio > 0:
        quality_parts.append("心得有紀錄但仍可再補充出場理由")
    else:
        quality_parts.append("幾乎沒有心得紀錄")

    result_part = "本輪結案獲利" if total_return_pct > 0 else ("本輪結案虧損" if total_return_pct < 0 else "本輪損益打平")
    return "；".join([result_part] + discipline_parts + quality_parts)


def summarize_closed_stock_trade_cycles(portfolio_id, stock_id, portfolio_hist_df=None):
    stock_id = normalize_stock_id(stock_id)
    stock_trades_df = trade_repository.list_trades(portfolio_id)
    if stock_trades_df.empty:
        return pd.DataFrame()

    stock_trades_df = stock_trades_df[
        stock_trades_df["stock_id"].apply(normalize_stock_id) == stock_id
    ].copy()
    if stock_trades_df.empty:
        return pd.DataFrame()

    stock_trades_df["date"] = pd.to_datetime(stock_trades_df["date"], errors="coerce")
    stock_trades_df["id"] = pd.to_numeric(stock_trades_df["id"], errors="coerce").fillna(0)
    stock_trades_df = stock_trades_df.dropna(subset=["date"]).sort_values(
        ["date", "id"], kind="stable"
    )
    stock_trades_df["date"] = stock_trades_df["date"].dt.strftime("%Y-%m-%d")

    journal_df = calculate_trade_journal(stock_trades_df)
    if journal_df.empty or not (journal_df["action"] == "Close").any():
        return pd.DataFrame()

    start_date = str(stock_trades_df.iloc[0]["date"])
    end_date = str(stock_trades_df.iloc[-1]["date"])
    refresh_price_snapshots_for_tickers([stock_id], start_date=start_date, end_date=end_date)
    price_data = get_price_snapshot_history([stock_id], start_date=start_date, end_date=end_date)
    if not price_data.empty:
        price_data = _normalize_history_index(price_data).ffill()

    if portfolio_hist_df is None or portfolio_hist_df.empty:
        portfolio_hist_df, _, _ = calculate_twr_and_nav(portfolio_id)
    allocation_weight_map = _build_allocation_weight_map(portfolio_hist_df, stock_id)
    nav_map = {}
    if portfolio_hist_df is not None and not portfolio_hist_df.empty:
        hist_copy = portfolio_hist_df.copy()
        hist_copy["Date"] = pd.to_datetime(hist_copy["Date"], errors="coerce")
        hist_copy = hist_copy.dropna(subset=["Date"])
        nav_map = {
            row["Date"].strftime("%Y-%m-%d"): float(row.get("NAV", 0) or 0)
            for _, row in hist_copy.iterrows()
        }

    closed_summaries = []
    cycle_rows = []
    cycle_no = 0
    running_shares = 0

    for _, journal_row in journal_df.iterrows():
        action = str(journal_row.get("action", "")).strip()
        prev_shares = running_shares
        current_shares = int(journal_row.get("accum_shares", 0) or 0)
        if prev_shares <= 0 and action in ["Buy", "Setup", "Add"]:
            cycle_rows = []
            cycle_no += 1
        cycle_rows.append(dict(journal_row))
        running_shares = current_shares

        if action != "Close" or running_shares > 0 or not cycle_rows:
            continue

        cycle_df = pd.DataFrame(cycle_rows).copy()
        cycle_start = str(cycle_df.iloc[0]["date"])
        cycle_end = str(cycle_df.iloc[-1]["date"])
        date_range = [
            d.strftime("%Y-%m-%d")
            for d in pd.date_range(pd.to_datetime(cycle_start), pd.to_datetime(cycle_end), freq="D")
        ]

        row_idx = 0
        shares_now = 0
        avg_cost_now = 0.0
        daily_records = []
        for daily_date in date_range:
            while row_idx < len(cycle_df) and str(cycle_df.iloc[row_idx]["date"]) <= daily_date:
                event_row = cycle_df.iloc[row_idx]
                shares_now = int(event_row.get("accum_shares", 0) or 0)
                avg_cost_now = float(event_row.get("avg_cost_after", event_row.get("avg_cost", 0)) or 0)
                row_idx += 1

            if shares_now <= 0:
                continue

            close_price = _resolve_stock_price_on_date(
                price_data,
                stock_id,
                daily_date,
                fallback_price=avg_cost_now,
            )
            invested_cost = shares_now * avg_cost_now
            floating_pnl = (close_price - avg_cost_now) * shares_now
            floating_pct = (
                floating_pnl / invested_cost * 100
                if invested_cost > 0
                else 0.0
            )
            nav_value = float(nav_map.get(daily_date, 0) or 0)
            cost_ratio_pct = (
                invested_cost / nav_value * 100 if nav_value > 0 else np.nan
            )
            daily_records.append(
                {
                    "date": daily_date,
                    "shares": shares_now,
                    "avg_cost": avg_cost_now,
                    "close_price": close_price,
                    "invested_cost": invested_cost,
                    "cost_ratio_pct": cost_ratio_pct,
                    "allocation_weight_pct": float(allocation_weight_map.get(daily_date, 0) or 0),
                    "floating_pnl": floating_pnl,
                    "floating_pct": floating_pct,
                }
            )

        daily_path_df = pd.DataFrame(daily_records)
        entry_rows = cycle_df[cycle_df["action"].isin(["Buy", "Add", "Setup"])].copy()
        exit_rows = cycle_df[cycle_df["action"].isin(["Reduce", "Close"])].copy()
        entry_amount = float((entry_rows["price"] * entry_rows["shares"]).sum()) if not entry_rows.empty else 0.0
        exit_amount = float((exit_rows["price"] * exit_rows["shares"]).sum()) if not exit_rows.empty else 0.0
        total_dividends = float(
            cycle_df.loc[cycle_df["action"] == "Dividend", "realized_pnl"].sum()
        )
        total_pnl = float(cycle_df.iloc[-1]["cum_pnl"]) - float(
            cycle_df.iloc[0]["cum_pnl"] - cycle_df.iloc[0]["realized_pnl"]
        )
        return_pct = total_pnl / entry_amount * 100 if entry_amount > 0 else 0.0
        max_invested_cost = (
            float(daily_path_df["invested_cost"].max()) if not daily_path_df.empty else entry_amount
        )
        max_cost_ratio_pct = (
            float(pd.to_numeric(daily_path_df["cost_ratio_pct"], errors="coerce").max())
            if not daily_path_df.empty
            else np.nan
        )
        max_alloc_weight_pct = (
            float(daily_path_df["allocation_weight_pct"].max()) if not daily_path_df.empty else np.nan
        )
        max_floating_pnl = (
            float(daily_path_df["floating_pnl"].max()) if not daily_path_df.empty else 0.0
        )
        min_floating_pnl = (
            min(0.0, float(daily_path_df["floating_pnl"].min()))
            if not daily_path_df.empty
            else 0.0
        )
        max_floating_pct = (
            float(daily_path_df.loc[daily_path_df["floating_pnl"].idxmax(), "floating_pct"])
            if not daily_path_df.empty
            else 0.0
        )
        min_floating_pct = (
            min(
                0.0,
                float(
                    daily_path_df.loc[
                        daily_path_df["floating_pnl"].idxmin(),
                        "floating_pct",
                    ]
                ),
            )
            if not daily_path_df.empty
            else 0.0
        )
        tp_candidates = [
            _normalize_optional_price_target(v)
            for v in cycle_df.get("take_profit_price", pd.Series(dtype=float)).tolist()
        ]
        sl_candidates = [
            _normalize_optional_price_target(v)
            for v in cycle_df.get("stop_loss_price", pd.Series(dtype=float)).tolist()
        ]
        notes_targets = [
            _parse_trade_risk_targets_from_notes(note)
            for note in cycle_df.get("trading_notes", pd.Series(dtype=str)).tolist()
        ]
        tp_candidates.extend(tp for tp, _ in notes_targets)
        sl_candidates.extend(sl for _, sl in notes_targets)
        take_profit_target = next((v for v in reversed(tp_candidates) if v is not None), None)
        stop_loss_target = next((v for v in reversed(sl_candidates) if v is not None), None)
        score_values = []
        for score_col in ["technical_score", "chip_score", "theme_score"]:
            if score_col in cycle_df.columns:
                score_values.extend(
                    pd.to_numeric(cycle_df[score_col], errors="coerce").dropna().tolist()
                )
        avg_score = float(np.mean(score_values)) if score_values else 0.0
        note_nonempty = [
            bool(strip_text)
            for strip_text in cycle_df.get("trading_notes", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .map(lambda t: _compose_trade_notes_with_risk(t).strip())
        ]
        notes_fill_ratio = (
            sum(note_nonempty) / len(note_nonempty) * 100 if note_nonempty else 0.0
        )

        summary_row = {
            "輪次": cycle_no,
            "進場日": cycle_start,
            "出場日": cycle_end,
            "持有天數": max(
                1,
                (pd.to_datetime(cycle_end) - pd.to_datetime(cycle_start)).days + 1,
            ),
            "投入資金": entry_amount,
            "出場回收": exit_amount + total_dividends,
            "結算損益": total_pnl,
            "結算報酬率 (%)": return_pct,
            "最大投入成本": max_invested_cost,
            "最大投入佔NAV (%)": max_cost_ratio_pct,
            "最大持倉比重 (%)": max_alloc_weight_pct,
            "最大浮盈": max_floating_pnl,
            "最大浮盈率 (%)": max_floating_pct,
            "最大浮虧": min_floating_pnl,
            "最大浮虧率 (%)": min_floating_pct,
            "停利目標價": take_profit_target,
            "停損目標價": stop_loss_target,
            "平均操作評分": avg_score,
            "心得填寫率 (%)": notes_fill_ratio,
            "進場均價": float(entry_amount / entry_rows["shares"].sum()) if not entry_rows.empty and entry_rows["shares"].sum() > 0 else 0.0,
            "出場均價": float(exit_amount / exit_rows["shares"].sum()) if not exit_rows.empty and exit_rows["shares"].sum() > 0 else 0.0,
            "過程日資料": json.dumps(daily_records, ensure_ascii=False),
        }
        summary_row["操作總和評估"] = _build_trade_cycle_evaluation(summary_row)
        closed_summaries.append(summary_row)
        cycle_rows = []

    return pd.DataFrame(closed_summaries)


def get_holdings_detail(portfolio_id):
    t_df = trade_repository.list_trades(portfolio_id)

    if t_df.empty:
        return pd.DataFrame(), []

    t_df["stock_id"] = t_df["stock_id"].apply(normalize_stock_id)
    active_holdings = []
    valuation_date = get_latest_tw_trading_date()

    for s in t_df["stock_id"].unique():
        sub_df = t_df[t_df["stock_id"] == s].copy()
        p_df = calculate_trade_journal(sub_df)
        last_row = p_df.iloc[-1]
        if last_row["accum_shares"] > 0:
            holding_start_date, holding_days, take_profit_price, stop_loss_price = _extract_position_meta(
                p_df,
                fallback_date=valuation_date,
            )
            active_holdings.append(
                {
                    "Stock": s,
                    "Shares": last_row["accum_shares"],
                    "AvgCost": last_row["avg_cost"],
                    "LastTradePrice": float(sub_df.iloc[-1]["price"]),
                    "HoldingStartDate": holding_start_date,
                    "HoldingDays": holding_days,
                    "TakeProfitPrice": take_profit_price,
                    "StopLossPrice": stop_loss_price,
                }
            )

    if not active_holdings:
        return pd.DataFrame(), []

    tickers = [h["Stock"] for h in active_holdings]
    manual_overrides = get_manual_price_overrides(tickers)
    price_df, _, _ = fetch_yfinance_history(tickers, period="5d")
    display_names = get_stock_display_names(tickers)

    results = []
    for h in active_holdings:
        sid = normalize_stock_id(h["Stock"])
        disp_name = display_names.get(sid, sid)
        current_price = 0.0

        manual_price = manual_overrides.get(sid, np.nan)
        if pd.notna(manual_price) and manual_price > 0:
            current_price = float(manual_price)
            price_source = "手動覆蓋"
        else:
            finmind_price = fetch_finmind_last_close(sid)
            if finmind_price > 0:
                current_price = finmind_price
                price_source = "FinMind"
            else:
                if not price_df.empty and sid in price_df.columns:
                    valid_p = price_df[sid].dropna()
                    if not valid_p.empty:
                        current_price = float(valid_p.iloc[-1])

                if current_price <= 0:
                    for candidate in get_symbol_candidates(sid):
                        try:
                            t_obj = yf.Ticker(candidate)
                            fast_info = getattr(t_obj, "fast_info", None)
                            if fast_info:
                                current_price = float(fast_info.get("lastPrice", 0) or 0)
                            if current_price <= 0:
                                hist_last = t_obj.history(period="5d", auto_adjust=True)
                                if not hist_last.empty:
                                    current_price = float(hist_last["Close"].dropna().iloc[-1])
                            if current_price > 0:
                                update_stock_full_symbol(sid, candidate)
                                break
                        except Exception:
                            current_price = 0.0
                price_source = "即時報價" if current_price > 0 else "無即時報價"

        if current_price > 0:
            final_p_display = current_price
            market_value = final_p_display * h["Shares"]
            total_cost = h["AvgCost"] * h["Shares"]
            unrealized_pnl = market_value - total_cost
            unrealized_pnl_pct = (unrealized_pnl / total_cost * 100) if total_cost > 0 else 0
        else:
            fallback_price = h["LastTradePrice"] if h["LastTradePrice"] > 0 else h["AvgCost"]
            if fallback_price > 0:
                final_p_display = fallback_price
                market_value = fallback_price * h["Shares"]
                total_cost = h["AvgCost"] * h["Shares"]
                unrealized_pnl = market_value - total_cost
                unrealized_pnl_pct = (unrealized_pnl / total_cost * 100) if total_cost > 0 else 0
                price_source = "估算報價"
            else:
                final_p_display = np.nan
                market_value = 0.0
                unrealized_pnl = 0.0
                unrealized_pnl_pct = np.nan
                price_source = "無法估值"

        results.append(
            {
                "標的": sid,
                "標的名稱": disp_name,
                "持有股數": h["Shares"],
                "持有起始日": h["HoldingStartDate"],
                "持有天數": h["HoldingDays"],
                "平均成本": round(h["AvgCost"], 1),
                "當前現價": final_p_display,
                "手動覆蓋價": manual_price if pd.notna(manual_price) else np.nan,
                "報價來源": price_source,
                "總市值": round(market_value, 1),
                "未實現損益 ($)": round(unrealized_pnl, 1),
                "未實現報酬率 (%)": unrealized_pnl_pct,
                "報酬效率 (%/日)": (
                    unrealized_pnl_pct / max(float(h["HoldingDays"] or 1), 1.0)
                    if pd.notna(unrealized_pnl_pct)
                    else np.nan
                ),
                "停利目標價": h["TakeProfitPrice"],
                "停損目標價": h["StopLossPrice"],
                "距停利 (%)": (
                    ((float(h["TakeProfitPrice"]) / final_p_display) - 1.0) * 100
                    if h["TakeProfitPrice"] is not None
                    and pd.notna(final_p_display)
                    and float(final_p_display) > 0
                    else np.nan
                ),
                "距停損 (%)": (
                    ((float(h["StopLossPrice"]) / final_p_display) - 1.0) * 100
                    if h["StopLossPrice"] is not None
                    and pd.notna(final_p_display)
                    and float(final_p_display) > 0
                    else np.nan
                ),
            }
        )

    return pd.DataFrame(results), active_holdings


GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.0-flash-lite", "gemini-2.0-flash"]
GEMINI_REVIEW_MODELS = ["gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.0-flash"]


def _get_gemini_client():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key or api_key == "your_api_key_here":
        return None
    try:
        from google import genai

        return genai.Client(api_key=api_key)
    except Exception:
        return None


def _call_gemini_with_fallback(image_bytes, prompt):
    client = _get_gemini_client()
    if client is None:
        return None, "no_key"

    from google.genai import types

    last_error = None
    for model_name in GEMINI_MODELS:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=[
                    types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg"),
                    prompt,
                ],
            )
            return response.text.strip(), None
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                last_error = "quota"
                continue
            last_error = err_str
            break
    return None, last_error


def _call_gemini_text_with_fallback(prompt):
    client = _get_gemini_client()
    if client is None:
        return None, "no_key"

    try:
        from google.genai import types
    except Exception:
        types = None

    last_error = None
    for model_name in GEMINI_REVIEW_MODELS:
        try:
            kwargs = {"model": model_name, "contents": prompt}
            if types is not None and hasattr(types, "GenerateContentConfig"):
                kwargs["config"] = types.GenerateContentConfig(
                    temperature=0.25,
                    max_output_tokens=4096,
                )
            response = client.models.generate_content(**kwargs)
            return str(response.text or "").strip(), None
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                last_error = "quota"
                continue
            last_error = err_str
            continue
    return None, last_error


def _to_json_safe_value(value):
    if isinstance(value, dict):
        return {str(k): _to_json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_safe_value(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S") if not pd.isna(value) else None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, np.generic):
        return _to_json_safe_value(value.item())
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def ai_review_closed_trade_cycle(stock_display_name, cycle_summary_row, cycle_trade_df):
    if cycle_summary_row is None or len(cycle_summary_row) == 0:
        return "這一輪目前沒有可分析的結案資料。"

    cycle_payload = _to_json_safe_value({
        key: cycle_summary_row.get(key)
        for key in [
            "輪次",
            "進場日",
            "出場日",
            "持有天數",
            "投入資金",
            "出場回收",
            "結算損益",
            "結算報酬率 (%)",
            "最大投入成本",
            "最大投入佔NAV (%)",
            "最大持倉比重 (%)",
            "最大浮盈",
            "最大浮盈率 (%)",
            "最大浮虧",
            "最大浮虧率 (%)",
            "停利目標價",
            "停損目標價",
            "平均操作評分",
            "進場均價",
            "出場均價",
            "操作總和評估",
        ]
    })
    try:
        cycle_payload["過程日資料"] = _to_json_safe_value(
            json.loads(cycle_summary_row.get("過程日資料", "[]") or "[]")
        )
    except Exception:
        cycle_payload["過程日資料"] = []

    review_trade_df = cycle_trade_df.copy() if cycle_trade_df is not None else pd.DataFrame()
    if not review_trade_df.empty:
        if "date" in review_trade_df.columns:
            review_trade_df["date"] = review_trade_df["date"].astype(str)
        if "trading_notes" in review_trade_df.columns:
            review_trade_df["trading_notes"] = (
                review_trade_df["trading_notes"]
                .fillna("")
                .astype(str)
                .map(lambda note: "\n".join(
                    line
                    for line in note.splitlines()
                    if not line.strip().startswith(TRADE_RISK_META_PREFIX)
                ).strip())
            )
        cycle_trade_payload = _to_json_safe_value(
            review_trade_df.to_dict("records")
        )
    else:
        cycle_trade_payload = []

    prompt = f"""
你是一位非常嚴格、重視紀律與風險控管的台股交易教練。請根據下方「單一標的單輪結案交易資料」，做一份完整、具體、可操作的交易復盤。

分析標的：{stock_display_name}

【本輪結案統計 JSON】
{json.dumps(cycle_payload, ensure_ascii=False, indent=2, default=_to_json_safe_value)}

【本輪逐筆交易與心得 JSON】
{json.dumps(cycle_trade_payload, ensure_ascii=False, indent=2, default=_to_json_safe_value)}

請用繁體中文回答，務必輸出以下 5 個段落，內容要具體、篇幅充足、不要只寫空泛形容詞：
1. AI 操作評分：請分別給「進出場紀律 / 加減碼節奏 / 風險控管 / 策略一致性 / 心得完整度」1~10 分，最後給總分與一句結論。
2. 這輪做對什麼：明確指出哪些操作值得保留。
3. 這輪最需要檢討什麼：若有追價、太晚停利、沒守停損、加碼理由薄弱、出場不一致，請直接點出。
4. 若重做一次，你會怎麼優化：請提出下一次可直接執行的規則。
5. 給使用者的下一輪提醒清單：列出 5~8 條短句提醒。
"""

    text, error = _call_gemini_text_with_fallback(prompt)
    if error == "no_key":
        return f"""### 系統復盤摘要
- AI 模型目前未啟用，先用系統規則版總評。
- 本輪結果：{cycle_payload.get('操作總和評估', '-')}
- 結案損益：${float(cycle_payload.get('結算損益', 0) or 0):+,.0f}
- 結案報酬率：{float(cycle_payload.get('結算報酬率 (%)', 0) or 0):+.2f}%
- 最大浮盈 / 最大浮虧：{float(cycle_payload.get('最大浮盈率 (%)', 0) or 0):+.2f}% / {float(cycle_payload.get('最大浮虧率 (%)', 0) or 0):+.2f}%
"""
    if error == "quota":
        return "⚠️ AI 使用配額已達今日上限，請明天再試；目前可先參考上方系統規則總評。"
    if error:
        return f"⚠️ AI 復盤產生失敗：{error}\n\n系統規則總評：{cycle_payload.get('操作總和評估', '-')}"
    return text or f"系統規則總評：{cycle_payload.get('操作總和評估', '-')}"


def get_trade_cycle_ai_review(portfolio_id, stock_id, cycle_no):
    stock_id = normalize_stock_id(stock_id)
    return trade_cycle_review_repository.get_review(portfolio_id, stock_id, cycle_no)


def save_trade_cycle_ai_review(portfolio_id, stock_id, cycle_no, review_text):
    stock_id = normalize_stock_id(stock_id)
    return trade_cycle_review_repository.upsert_review(
        portfolio_id,
        stock_id,
        cycle_no,
        review_text,
    )


def ai_vision_single_trade(image_bytes):
    if _get_gemini_client() is None:
        return [
            {
                "trade_date": "",
                "side": "buy",
                "stock_id": "2308",
                "price": 1375.0,
                "shares": 25,
                "_mock": True,
            }
        ]

    prompt = """你是一個台灣股票交易資料擷取助手。
請仔細閱讀圖片中的交易紀錄（可能是券商 App 或對帳單截圖），擷取所有交易資訊：
- trade_date: 交易日期，格式 YYYY-MM-DD；如果圖片中看不清楚或沒有日期，請填空字串
- side: 只需判斷這筆是 buy 或 sell，請不要判斷 Buy / Add / Reduce / Close
- stock_id: 純數字股票代號（例如 2330，不含 .TW 或中文名稱）
- price: 成交均價（數字，保留小數點後最多2位）
- shares: 成交股數（整數）

如果圖片中有多筆交易，請回傳 JSON 陣列；如果只有一筆，回傳 JSON 物件或陣列皆可。
請嚴格回傳 JSON 格式，不要加任何說明文字。
範例格式：
[{"trade_date": "2026-04-16", "side": "buy", "stock_id": "2330", "price": 750.0, "shares": 1000}, ...]"""

    text, error = _call_gemini_with_fallback(image_bytes, prompt)
    if error == "quota":
        st.error("⚠️ AI 使用配額已達今日上限，請明天再試或至 Google AI Studio 升級方案。")
        return None
    if error:
        st.error(f"⚠️ AI 辨識失敗：{error}")
        return None

    try:
        text = re.sub(r"^```[\w]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            parsed_list = [parsed]
        elif isinstance(parsed, list):
            parsed_list = parsed
        else:
            return None

        for item in parsed_list:
            item["stock_id"] = normalize_stock_id(str(item.get("stock_id", "")))
            item["trade_date"] = _normalize_ai_trade_date(
                item.get("trade_date") or item.get("date")
            )
            item["side"] = _normalize_ai_trade_side(
                item.get("side") or item.get("action") or item.get("direction")
            )
        return parsed_list
    except Exception:
        st.error("⚠️ AI 回傳格式無法解析，請稍後再試或手動填寫。")
        return None


def ai_vision_portfolio(image_bytes):
    if _get_gemini_client() is None:
        return [
            {"股票代碼 (純代號)": "2330", "持股均價": 750.0, "持有股數": 1000},
            {"股票代碼 (純代號)": "2308", "持股均價": 1375.0, "持有股數": 500},
        ]

    prompt = """你是一個台灣股票庫存資料擷取助手。
請仔細閱讀圖片中的庫存總覽（可能是券商 App 庫存頁面截圖），擷取所有持股的以下資訊：
- 股票代碼 (純代號): 純數字股票代號（例如 2330，不含 .TW 或中文名稱）
- 持股均價: 持股成本均價（數字，保留小數點後最多2位）
- 持有股數: 目前庫存股數（整數）

請嚴格回傳以下格式的 JSON 陣列，不要加任何說明文字：
[{"股票代碼 (純代號)": "2330", "持股均價": 750.0, "持有股數": 1000}, ...]"""

    text, error = _call_gemini_with_fallback(image_bytes, prompt)
    if error == "quota":
        st.error("⚠️ AI 使用配額已達今日上限，請明天再試或至 Google AI Studio 升級方案。您仍可手動在下方表單填寫庫存資料。")
        return None
    if error:
        st.error(f"⚠️ AI 辨識失敗：{error}")
        return None

    try:
        text = re.sub(r"^```[\w]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        parsed_list = json.loads(text)
        for item in parsed_list:
            item["股票代碼 (純代號)"] = normalize_stock_id(
                str(item.get("股票代碼 (純代號)", ""))
            )
        return parsed_list
    except Exception:
        st.error("⚠️ AI 回傳格式無法解析，請稍後再試或手動填寫。")
        return None


def process_trade_derivation(portfolio_id, parsed_data):
    stock_id = normalize_stock_id(parsed_data.get("stock_id", ""))
    shares = int(pd.to_numeric(parsed_data.get("shares", 0), errors="coerce") or 0)
    trade_date = _normalize_ai_trade_date(
        parsed_data.get("trade_date") or parsed_data.get("date")
    )
    side = _normalize_ai_trade_side(
        parsed_data.get("side") or parsed_data.get("action") or parsed_data.get("direction")
    )

    if not stock_id:
        return "Buy"

    inv_df = get_inventory_as_of_date(
        portfolio_id,
        stock_id=stock_id,
        as_of_date=trade_date or None,
    )
    current_shares = 0
    if not inv_df.empty:
        current_shares = inv_df.iloc[0]["current_shares"]

    if side == "sell":
        if current_shares <= 0:
            return "Reduce"
        if shares > 0 and shares >= current_shares:
            return "Close"
        return "Reduce"

    return "Buy" if current_shares == 0 else "Add"
