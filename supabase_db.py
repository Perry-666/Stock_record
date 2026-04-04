import os
import re
import sqlite3

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_PAGE_SIZE = 1000
SUPABASE_WRITE_BATCH_SIZE = 500

try:
    from supabase import Client, create_client
except Exception:
    Client = None
    create_client = None


SUPABASE_TABLE_COLUMNS = {
    "Portfolios": [
        "id",
        "name",
        "t0_cash",
        "t2_cash",
        "initial_cash",
        "nav_dirty_from_date",
    ],
    "Trades": [
        "id",
        "portfolio_id",
        "date",
        "stock_id",
        "action",
        "price",
        "shares",
        "is_disposed",
        "basic_check",
        "tech_check",
        "chips_check",
        "industry_check",
        "trading_notes",
        "settled",
        "technical_score",
        "chip_score",
        "theme_score",
        "take_profit_price",
        "stop_loss_price",
    ],
    "CashFlows": ["id", "portfolio_id", "date", "type", "amount"],
    "MacroJournals": ["id", "date", "content"],
    "StockNames": ["stock_id", "name", "full_symbol"],
    "MarketHolidays": ["date", "reason", "is_settlement_open"],
    "ManualPriceOverrides": ["stock_id", "price", "updated_at"],
    "PriceSnapshots": ["date", "stock_id", "close_price", "source", "updated_at"],
    "DailyNavSnapshots": [
        "portfolio_id",
        "date",
        "nav",
        "cash",
        "holdings_value",
        "cash_flow",
        "daily_return",
        "twr",
        "benchmark_price",
        "benchmark_daily_return",
        "benchmark_cum_return",
        "daily_alpha",
        "cum_alpha",
        "drawdown",
        "notes",
        "allocation_json",
        "is_trading_day",
        "win_flag",
        "updated_at",
    ],
    "TradeCycleReviews": [
        "portfolio_id",
        "stock_id",
        "cycle_no",
        "review_text",
        "updated_at",
    ],
}

_supabase_client = None


def get_supabase_client():
    global _supabase_client

    if _supabase_client is not None:
        return _supabase_client

    if create_client is None:
        raise RuntimeError(
            "尚未安裝 Supabase Python 套件，請先執行 `pip install supabase`。"
        )

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "缺少 SUPABASE_URL / SUPABASE_KEY，請先在 .env 設定 Supabase 連線資訊。"
        )

    try:
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        _supabase_client.table("Portfolios").select("id").limit(1).execute()
    except Exception as exc:
        _supabase_client = None
        raise RuntimeError(f"Supabase 連線失敗：{exc}") from exc

    return _supabase_client


def _normalize_sql(sql):
    return " ".join(str(sql or "").strip().rstrip(";").split())


def _extract_in_list_literals(sql):
    match = re.search(r"\bIN\s*\((.+)\)", sql, flags=re.IGNORECASE)
    if not match:
        return []
    raw_items = match.group(1)
    return [
        item.strip().strip("'").strip('"')
        for item in raw_items.split(",")
        if item.strip() and "?" not in item
    ]


def _fetch_all_rows(table_name, columns="*", query_modifier=None):
    client = get_supabase_client()
    rows = []
    start = 0

    while True:
        query = client.table(table_name).select(columns)
        if query_modifier is not None:
            query = query_modifier(query)
        response = query.range(start, start + SUPABASE_PAGE_SIZE - 1).execute()
        page_rows = response.data or []
        rows.extend(page_rows)
        if len(page_rows) < SUPABASE_PAGE_SIZE:
            break
        start += SUPABASE_PAGE_SIZE

    return rows


def _chunked_records(records, chunk_size=SUPABASE_WRITE_BATCH_SIZE):
    for idx in range(0, len(records), chunk_size):
        yield records[idx : idx + chunk_size]


def _rows_to_description(rows, fallback_columns=None):
    if rows:
        cols = list(rows[0].keys())
    else:
        cols = list(fallback_columns or [])
    return [(col, None, None, None, None, None, None) for col in cols]


def _is_portfolio_duplicate_error(exc):
    message = str(exc).lower()
    return (
        "duplicate" in message
        or "unique" in message
        or "23505" in message
        or "already exists" in message
    )


class SupabaseRow(dict):
    def __iter__(self):
        return iter(self.values())


class SupabaseDBConnection:
    def __init__(self):
        self.row_factory = None
        self._rows = []
        self.description = []
        self.rowcount = -1
        self.lastrowid = None

    def cursor(self):
        return self

    def close(self):
        return None

    def commit(self):
        return None

    def rollback(self):
        return None

    def fetchone(self):
        return SupabaseRow(self._rows[0]) if self._rows else None

    def fetchall(self):
        return [SupabaseRow(row) for row in self._rows]

    def executemany(self, sql, seq_of_params):
        sql_clean = _normalize_sql(sql)
        sql_upper = sql_clean.upper()
        params_list = [tuple(params or ()) for params in seq_of_params]

        if not params_list:
            self.rowcount = 0
            return self

        client = get_supabase_client()

        if sql_upper.startswith("INSERT INTO PRICESNAPSHOTS"):
            records_map = {}
            for params in params_list:
                records_map[(params[0], params[1])] = {
                    "date": params[0],
                    "stock_id": params[1],
                    "close_price": params[2],
                    "source": params[3],
                    "updated_at": params[4],
                }
            records = list(records_map.values())
            for chunk in _chunked_records(records):
                client.table("PriceSnapshots").upsert(
                    chunk, on_conflict="date,stock_id"
                ).execute()
            self.rowcount = len(records)
            return self

        if sql_upper.startswith("INSERT INTO DAILYNAVSNAPSHOTS"):
            records_map = {}
            for params in params_list:
                records_map[(params[0], params[1])] = {
                    "portfolio_id": params[0],
                    "date": params[1],
                    "nav": params[2],
                    "cash": params[3],
                    "holdings_value": params[4],
                    "cash_flow": params[5],
                    "daily_return": params[6],
                    "twr": params[7],
                    "benchmark_price": params[8],
                    "benchmark_daily_return": params[9],
                    "benchmark_cum_return": params[10],
                    "daily_alpha": params[11],
                    "cum_alpha": params[12],
                    "drawdown": params[13],
                    "notes": params[14],
                    "allocation_json": params[15],
                    "is_trading_day": params[16],
                    "win_flag": params[17],
                    "updated_at": params[18],
                }
            records = list(records_map.values())
            for chunk in _chunked_records(records):
                client.table("DailyNavSnapshots").upsert(
                    chunk, on_conflict="portfolio_id,date"
                ).execute()
            self.rowcount = len(records)
            return self

        total_rowcount = 0
        last_id = None
        for params in params_list:
            cursor = self.execute(sql, params)
            total_rowcount += max(cursor.rowcount, 0)
            if cursor.lastrowid is not None:
                last_id = cursor.lastrowid
        self.rowcount = total_rowcount
        self.lastrowid = last_id
        return self

    def execute(self, sql, params=()):
        sql_clean = _normalize_sql(sql)
        sql_upper = sql_clean.upper()
        params = tuple(params or ())
        self._rows = []
        self.description = []
        self.rowcount = -1
        self.lastrowid = None
        client = get_supabase_client()

        if not sql_clean:
            return self

        if sql_upper.startswith("PRAGMA TABLE_INFO"):
            table_name = re.search(
                r"PRAGMA\s+table_info\(([^)]+)\)", sql_clean, flags=re.IGNORECASE
            )
            target_table = table_name.group(1).strip('"') if table_name else ""
            self._rows = [
                {"name": col} for col in SUPABASE_TABLE_COLUMNS.get(target_table, [])
            ]
            self.description = _rows_to_description(self._rows, ["name"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("CREATE TABLE") or sql_upper.startswith("ALTER TABLE"):
            self.rowcount = 0
            return self

        if "UPDATE TRADES SET SETTLED = CASE" in sql_upper:
            self.rowcount = 0
            return self

        if sql_upper.startswith("UPDATE PORTFOLIOS SET INITIAL_CASH = CASE"):
            self._backfill_default_portfolios_initial_cash()
            self.rowcount = 2
            return self

        if sql_upper == "SELECT * FROM PORTFOLIOS":
            self._rows = _fetch_all_rows("Portfolios", "*")
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["Portfolios"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith(
            "SELECT T0_CASH, T2_CASH FROM PORTFOLIOS WHERE ID ="
        ):
            self._rows = _fetch_all_rows(
                "Portfolios",
                "t0_cash,t2_cash",
                lambda q: q.eq("id", params[0]),
            )
            self.description = _rows_to_description(
                self._rows, ["t0_cash", "t2_cash"]
            )
            self.rowcount = len(self._rows)
            return self

        if (
            "SELECT DATE FROM MARKETHOLIDAYS" in sql_upper
            and "IS_SETTLEMENT_OPEN" in sql_upper
        ):
            self._rows = _fetch_all_rows(
                "MarketHolidays",
                "date",
                lambda q: q.eq("is_settlement_open", False),
            )
            self.description = _rows_to_description(self._rows, ["date"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("SELECT DATE FROM MARKETHOLIDAYS"):
            self._rows = _fetch_all_rows("MarketHolidays", "date")
            self.description = _rows_to_description(self._rows, ["date"])
            self.rowcount = len(self._rows)
            return self

        if "FROM MARKETHOLIDAYS ORDER BY DATE ASC" in sql_upper:
            rows = _fetch_all_rows(
                "MarketHolidays",
                "date,reason,is_settlement_open",
                lambda q: q.order("date", desc=False),
            )
            self._rows = [
                {
                    "date": row.get("date"),
                    "reason": row.get("reason") or "",
                    "is_settlement_open": row.get("is_settlement_open") or False,
                }
                for row in rows
            ]
            self.description = _rows_to_description(
                self._rows, ["date", "reason", "is_settlement_open"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("INSERT OR REPLACE INTO MARKETHOLIDAYS") or sql_upper.startswith(
            "INSERT OR IGNORE INTO MARKETHOLIDAYS"
        ):
            client.table("MarketHolidays").upsert(
                {
                    "date": params[0],
                    "reason": params[1] if len(params) > 1 else "",
                    "is_settlement_open": bool(params[2]) if len(params) > 2 else False,
                },
                on_conflict="date",
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("DELETE FROM MARKETHOLIDAYS WHERE DATE"):
            client.table("MarketHolidays").delete().eq("date", params[0]).execute()
            self.rowcount = 1
            return self

        if "FROM TRADES" in sql_upper and "SUM(" in sql_upper and "GROUP BY STOCK_ID" in sql_upper:
            rows = _fetch_all_rows(
                "Trades",
                "stock_id,action,shares,portfolio_id",
                lambda q: q.eq("portfolio_id", params[0]),
            )
            stock_filter = params[1] if len(params) > 1 else None
            inv_df = pd.DataFrame(rows)
            if inv_df.empty:
                self._rows = []
            else:
                if stock_filter:
                    inv_df = inv_df[inv_df["stock_id"] == stock_filter]
                inv_df["signed_shares"] = np.where(
                    inv_df["action"].isin(["Buy", "Add", "Setup"]),
                    pd.to_numeric(inv_df["shares"], errors="coerce").fillna(0),
                    np.where(
                        inv_df["action"].isin(["Reduce", "Close"]),
                        -pd.to_numeric(inv_df["shares"], errors="coerce").fillna(0),
                        0,
                    ),
                )
                grouped = (
                    inv_df.groupby("stock_id", as_index=False)["signed_shares"]
                    .sum()
                    .rename(columns={"signed_shares": "current_shares"})
                )
                grouped = grouped[grouped["current_shares"] > 0]
                self._rows = grouped.to_dict("records")
            self.description = _rows_to_description(
                self._rows, ["stock_id", "current_shares"]
            )
            self.rowcount = len(self._rows)
            return self

        if (
            "FROM TRADES" in sql_upper
            and "WHERE PORTFOLIO_ID = ?" in sql_upper
            and "SETTLED = 0" in sql_upper
            and "ORDER BY DATE ASC, ID ASC" in sql_upper
        ):
            raw_rows = _fetch_all_rows(
                "Trades",
                "date,stock_id,action,price,shares",
                lambda q: q.eq("portfolio_id", params[0])
                .eq("settled", 0)
                .order("date", desc=False)
                .order("id", desc=False),
            )
            self._rows = [
                {
                    "交易日": row.get("date"),
                    "標的": row.get("stock_id"),
                    "動作": row.get("action"),
                    "price": row.get("price"),
                    "shares": row.get("shares"),
                    "原始金額": float(row.get("price") or 0)
                    * float(row.get("shares") or 0),
                }
                for row in raw_rows
            ]
            self.description = _rows_to_description(
                self._rows, ["交易日", "標的", "動作", "price", "shares", "原始金額"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith(
            "SELECT NAV_DIRTY_FROM_DATE FROM PORTFOLIOS WHERE ID ="
        ):
            self._rows = _fetch_all_rows(
                "Portfolios",
                "nav_dirty_from_date",
                lambda q: q.eq("id", params[0]),
            )
            self.description = _rows_to_description(
                self._rows, ["nav_dirty_from_date"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith(
            "UPDATE PORTFOLIOS SET NAV_DIRTY_FROM_DATE = ? WHERE ID = ?"
        ):
            client.table("Portfolios").update(
                {"nav_dirty_from_date": params[0]}
            ).eq("id", params[1]).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("INSERT INTO TRADES"):
            record = {
                "portfolio_id": params[0],
                "date": params[1],
                "stock_id": params[2],
                "action": params[3],
                "price": params[4],
                "shares": params[5],
                "is_disposed": bool(params[6]),
                "basic_check": bool(params[7]),
                "tech_check": bool(params[8]),
                "chips_check": bool(params[9]),
                "industry_check": bool(params[10]),
                "trading_notes": params[11],
                "settled": int(params[12]) if len(params) > 12 else 0,
                "technical_score": int(params[13]) if len(params) > 13 else 3,
                "chip_score": int(params[14]) if len(params) > 14 else 3,
                "theme_score": int(params[15]) if len(params) > 15 else 3,
            }
            response = client.table("Trades").insert(record).execute()
            self._rows = response.data or []
            self.lastrowid = (
                self._rows[0].get("id") if self._rows else None
            )
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["Trades"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith(
            "SELECT COALESCE(INITIAL_CASH, 0) AS INITIAL_CASH FROM PORTFOLIOS WHERE ID="
        ):
            rows = _fetch_all_rows(
                "Portfolios",
                "initial_cash",
                lambda q: q.eq("id", params[0]),
            )
            self._rows = [
                {"initial_cash": float((rows[0] if rows else {}).get("initial_cash") or 0)}
            ]
            self.description = _rows_to_description(self._rows, ["initial_cash"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("SELECT TYPE, AMOUNT FROM CASHFLOWS WHERE PORTFOLIO_ID="):
            self._rows = _fetch_all_rows(
                "CashFlows",
                "type,amount",
                lambda q: q.eq("portfolio_id", params[0]),
            )
            self.description = _rows_to_description(self._rows, ["type", "amount"])
            self.rowcount = len(self._rows)
            return self

        if (
            sql_upper.startswith("SELECT ACTION, PRICE, SHARES FROM TRADES WHERE PORTFOLIO_ID=? AND SETTLED=1")
            or sql_upper.startswith("SELECT ACTION, PRICE, SHARES FROM TRADES WHERE PORTFOLIO_ID=? AND SETTLED = 1")
        ):
            self._rows = _fetch_all_rows(
                "Trades",
                "action,price,shares",
                lambda q: q.eq("portfolio_id", params[0]).eq("settled", 1),
            )
            self.description = _rows_to_description(
                self._rows, ["action", "price", "shares"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith(
            "SELECT ACTION, PRICE, SHARES FROM TRADES WHERE PORTFOLIO_ID="
        ):
            self._rows = _fetch_all_rows(
                "Trades",
                "action,price,shares",
                lambda q: q.eq("portfolio_id", params[0]),
            )
            self.description = _rows_to_description(
                self._rows, ["action", "price", "shares"]
            )
            self.rowcount = len(self._rows)
            return self

        trades_select_match = re.match(
            r"SELECT\s+(?P<cols>.+?)\s+FROM\s+TRADES\s+WHERE\s+PORTFOLIO_ID\s*=\s*\?\s+ORDER\s+BY\s+DATE\s+ASC(?:\s*,\s*ID\s+ASC)?$",
            sql_clean,
            flags=re.IGNORECASE,
        )
        if trades_select_match and "SUM(" not in sql_upper and "GROUP BY" not in sql_upper:
            selected_cols_raw = trades_select_match.group("cols").strip()
            selected_cols = (
                SUPABASE_TABLE_COLUMNS["Trades"]
                if selected_cols_raw == "*"
                else [col.strip().strip('"').lower() for col in selected_cols_raw.split(",")]
            )
            self._rows = _fetch_all_rows(
                "Trades",
                ",".join(selected_cols) if selected_cols_raw != "*" else "*",
                lambda q: q.eq("portfolio_id", params[0]).order("date", desc=False).order(
                    "id", desc=False
                ),
            )
            self.description = _rows_to_description(self._rows, selected_cols)
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("UPDATE PORTFOLIOS SET T0_CASH = ?, T2_CASH = ? WHERE ID = ?"):
            client.table("Portfolios").update(
                {"t0_cash": params[0], "t2_cash": params[1]}
            ).eq("id", params[2]).execute()
            self.rowcount = 1
            return self

        if (
            sql_upper.startswith("SELECT ID, DATE FROM TRADES")
            and "SETTLED = 0" in sql_upper
            and "IS_DISPOSED = 0" in sql_upper
        ):
            self._rows = _fetch_all_rows(
                "Trades",
                "id,date",
                lambda q: q.eq("portfolio_id", params[0])
                .eq("settled", 0)
                .eq("is_disposed", False),
            )
            self.description = _rows_to_description(self._rows, ["id", "date"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("UPDATE TRADES SET SETTLED = 1 WHERE ID = ?"):
            client.table("Trades").update({"settled": 1}).eq("id", params[0]).execute()
            self.rowcount = 1
            return self

        if (
            sql_upper.startswith("SELECT DATE, ACTION FROM TRADES WHERE ID = ? AND PORTFOLIO_ID = ?")
            or sql_upper.startswith("SELECT DATE, ACTION FROM TRADES WHERE ID = ? AND PORTFOLIO_ID = ?")
        ):
            self._rows = _fetch_all_rows(
                "Trades",
                "date,action",
                lambda q: q.eq("id", params[0]).eq("portfolio_id", params[1]).limit(1),
            )
            self.description = _rows_to_description(self._rows, ["date", "action"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("UPDATE TRADES SET DATE = ?"):
            client.table("Trades").update(
                {
                    "date": params[0],
                    "stock_id": params[1],
                    "action": params[2],
                    "price": params[3],
                    "shares": params[4],
                    "is_disposed": bool(params[5]),
                    "trading_notes": params[6],
                    "settled": int(params[7]),
                    "technical_score": int(params[8]),
                    "chip_score": int(params[9]),
                    "theme_score": int(params[10]),
                }
            ).eq("id", params[11]).eq("portfolio_id", params[12]).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("INSERT INTO CASHFLOWS"):
            response = client.table("CashFlows").insert(
                {
                    "portfolio_id": params[0],
                    "date": params[1],
                    "type": params[2],
                    "amount": params[3],
                }
            ).execute()
            self._rows = response.data or []
            self.lastrowid = self._rows[0].get("id") if self._rows else None
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["CashFlows"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("SELECT * FROM MACROJOURNALS ORDER BY DATE DESC"):
            self._rows = _fetch_all_rows(
                "MacroJournals",
                "*",
                lambda q: q.order("date", desc=True),
            )
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["MacroJournals"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("INSERT OR REPLACE INTO MACROJOURNALS"):
            client.table("MacroJournals").upsert(
                {"date": params[0], "content": params[1]},
                on_conflict="date",
            ).execute()
            self.rowcount = 1
            return self

        if (
            "SELECT STOCK_ID, PRICE FROM MANUALPRICEOVERRIDES" in sql_upper
            and "WHERE STOCK_ID IN" in sql_upper
        ):
            stock_ids = list(params)
            self._rows = _fetch_all_rows(
                "ManualPriceOverrides",
                "stock_id,price",
                lambda q: q.in_("stock_id", stock_ids),
            )
            self.description = _rows_to_description(self._rows, ["stock_id", "price"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("SELECT STOCK_ID, PRICE FROM MANUALPRICEOVERRIDES"):
            self._rows = _fetch_all_rows("ManualPriceOverrides", "stock_id,price")
            self.description = _rows_to_description(self._rows, ["stock_id", "price"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("INSERT OR REPLACE INTO MANUALPRICEOVERRIDES"):
            client.table("ManualPriceOverrides").upsert(
                {
                    "stock_id": params[0],
                    "price": params[1],
                    "updated_at": params[2],
                },
                on_conflict="stock_id",
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("DELETE FROM MANUALPRICEOVERRIDES WHERE STOCK_ID"):
            client.table("ManualPriceOverrides").delete().eq(
                "stock_id", params[0]
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("SELECT FULL_SYMBOL FROM STOCKNAMES WHERE STOCK_ID ="):
            self._rows = _fetch_all_rows(
                "StockNames",
                "full_symbol",
                lambda q: q.eq("stock_id", params[0]),
            )
            self.description = _rows_to_description(self._rows, ["full_symbol"])
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("UPDATE STOCKNAMES SET FULL_SYMBOL = ? WHERE STOCK_ID = ?"):
            client.table("StockNames").update(
                {"full_symbol": params[0]}
            ).eq("stock_id", params[1]).execute()
            self.rowcount = 1
            return self

        if "SELECT STOCK_ID, NAME, FULL_SYMBOL FROM STOCKNAMES WHERE STOCK_ID IN" in sql_upper:
            in_items = list(params) if params else _extract_in_list_literals(sql_clean)
            self._rows = _fetch_all_rows(
                "StockNames",
                "stock_id,name,full_symbol",
                lambda q: q.in_("stock_id", in_items),
            )
            self.description = _rows_to_description(
                self._rows, ["stock_id", "name", "full_symbol"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("INSERT OR REPLACE INTO STOCKNAMES"):
            client.table("StockNames").upsert(
                {
                    "stock_id": params[0],
                    "name": params[1],
                    "full_symbol": params[2],
                },
                on_conflict="stock_id",
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("INSERT INTO PRICESNAPSHOTS"):
            payload = {
                "date": params[0],
                "stock_id": params[1],
                "close_price": params[2],
                "source": params[3],
                "updated_at": params[4],
            }
            client.table("PriceSnapshots").upsert(
                payload,
                on_conflict="date,stock_id",
            ).execute()
            self.rowcount = 1
            return self

        if "SELECT DATE, STOCK_ID, CLOSE_PRICE FROM PRICESNAPSHOTS WHERE STOCK_ID IN" in sql_upper:
            stock_params = list(params)
            start_date = None
            end_date = None
            if " AND date <= ?" in sql_clean:
                end_date = stock_params.pop(-1)
            if " AND date >= ?" in sql_clean:
                start_date = stock_params.pop(-1)
            stock_ids = stock_params

            def _modifier(q):
                q = q.in_("stock_id", stock_ids)
                if start_date:
                    q = q.gte("date", start_date)
                if end_date:
                    q = q.lte("date", end_date)
                return q.order("date", desc=False)

            self._rows = _fetch_all_rows(
                "PriceSnapshots",
                "date,stock_id,close_price",
                _modifier,
            )
            self.description = _rows_to_description(
                self._rows, ["date", "stock_id", "close_price"]
            )
            self.rowcount = len(self._rows)
            return self

        if "SELECT DISTINCT STOCK_ID FROM TRADES WHERE PORTFOLIO_ID = ?" in sql_upper:
            rows = _fetch_all_rows(
                "Trades",
                "stock_id",
                lambda q: q.eq("portfolio_id", params[0]),
            )
            stock_ids = sorted({row.get("stock_id") for row in rows if row.get("stock_id")})
            self._rows = [{"stock_id": sid} for sid in stock_ids]
            self.description = _rows_to_description(self._rows, ["stock_id"])
            self.rowcount = len(self._rows)
            return self

        if "SELECT COUNT(DISTINCT DATE) AS DATES, MAX(DATE) AS LATEST_DATE FROM PRICESNAPSHOTS" in sql_upper:
            stock_ids = list(params)
            rows = _fetch_all_rows(
                "PriceSnapshots",
                "date,stock_id",
                lambda q: q.in_("stock_id", stock_ids),
            )
            df = pd.DataFrame(rows)
            if df.empty:
                self._rows = [{"dates": 0, "latest_date": None}]
            else:
                self._rows = [
                    {
                        "dates": int(df["date"].nunique()),
                        "latest_date": df["date"].max(),
                    }
                ]
            self.description = _rows_to_description(
                self._rows, ["dates", "latest_date"]
            )
            self.rowcount = 1
            return self

        if "SELECT STOCK_ID, MAX(DATE) AS LATEST_DATE FROM PRICESNAPSHOTS" in sql_upper:
            stock_ids = list(params)
            rows = _fetch_all_rows(
                "PriceSnapshots",
                "stock_id,date",
                lambda q: q.in_("stock_id", stock_ids),
            )
            df = pd.DataFrame(rows)
            if df.empty:
                self._rows = []
            else:
                agg_df = (
                    df.groupby("stock_id", as_index=False)["date"]
                    .max()
                    .rename(columns={"date": "latest_date"})
                )
                self._rows = agg_df.to_dict("records")
            self.description = _rows_to_description(
                self._rows, ["stock_id", "latest_date"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("DELETE FROM DAILYNAVSNAPSHOTS WHERE PORTFOLIO_ID = ? AND DATE >= ?"):
            client.table("DailyNavSnapshots").delete().eq("portfolio_id", params[0]).gte(
                "date", params[1]
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("DELETE FROM DAILYNAVSNAPSHOTS WHERE PORTFOLIO_ID = ?"):
            client.table("DailyNavSnapshots").delete().eq("portfolio_id", params[0]).execute()
            self.rowcount = 1
            return self

        if (
            sql_upper.startswith("SELECT")
            and "FROM DAILYNAVSNAPSHOTS" in sql_upper
            and "WHERE PORTFOLIO_ID = ?" in sql_upper
        ):
            rows = _fetch_all_rows(
                "DailyNavSnapshots",
                (
                    "date,nav,cash_flow,cash,holdings_value,daily_return,twr,"
                    "benchmark_price,notes,is_trading_day,benchmark_daily_return,"
                    "benchmark_cum_return,drawdown,daily_alpha,cum_alpha,"
                    "allocation_json,win_flag"
                ),
                lambda q: q.eq("portfolio_id", params[0]).order("date", desc=False),
            )
            self._rows = [
                {
                    "Date": row.get("date"),
                    "NAV": row.get("nav"),
                    "CashFlow": row.get("cash_flow"),
                    "Cash": row.get("cash"),
                    "Holdings": row.get("holdings_value"),
                    "DailyReturn": row.get("daily_return"),
                    "TWR": row.get("twr"),
                    "BenchmarkPrice": row.get("benchmark_price"),
                    "Notes": row.get("notes"),
                    "IsTradingDay": row.get("is_trading_day"),
                    "BenchmarkDailyReturn": row.get("benchmark_daily_return"),
                    "BenchmarkCumReturn": row.get("benchmark_cum_return"),
                    "Drawdown": row.get("drawdown"),
                    "DailyAlpha": row.get("daily_alpha"),
                    "CumAlpha": row.get("cum_alpha"),
                    "AllocationJson": row.get("allocation_json"),
                    "Win?": row.get("win_flag"),
                }
                for row in rows
            ]
            self.description = _rows_to_description(
                self._rows,
                [
                    "Date",
                    "NAV",
                    "CashFlow",
                    "Cash",
                    "Holdings",
                    "DailyReturn",
                    "TWR",
                    "BenchmarkPrice",
                    "Notes",
                    "IsTradingDay",
                    "BenchmarkDailyReturn",
                    "BenchmarkCumReturn",
                    "Drawdown",
                    "DailyAlpha",
                    "CumAlpha",
                    "AllocationJson",
                    "Win?",
                ],
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("INSERT INTO DAILYNAVSNAPSHOTS"):
            client.table("DailyNavSnapshots").upsert(
                {
                    "portfolio_id": params[0],
                    "date": params[1],
                    "nav": params[2],
                    "cash": params[3],
                    "holdings_value": params[4],
                    "cash_flow": params[5],
                    "daily_return": params[6],
                    "twr": params[7],
                    "benchmark_price": params[8],
                    "benchmark_daily_return": params[9],
                    "benchmark_cum_return": params[10],
                    "daily_alpha": params[11],
                    "cum_alpha": params[12],
                    "drawdown": params[13],
                    "notes": params[14],
                    "allocation_json": params[15],
                    "is_trading_day": params[16],
                    "win_flag": params[17],
                    "updated_at": params[18],
                },
                on_conflict="portfolio_id,date",
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("UPDATE PORTFOLIOS SET NAV_DIRTY_FROM_DATE = NULL WHERE ID = ?"):
            client.table("Portfolios").update(
                {"nav_dirty_from_date": None}
            ).eq("id", params[0]).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith(
            "SELECT COALESCE(INITIAL_CASH, 0) AS INITIAL_CASH, NAV_DIRTY_FROM_DATE FROM PORTFOLIOS WHERE ID = ?"
        ):
            rows = _fetch_all_rows(
                "Portfolios",
                "initial_cash,nav_dirty_from_date",
                lambda q: q.eq("id", params[0]),
            )
            row = rows[0] if rows else {}
            self._rows = [
                {
                    "initial_cash": float(row.get("initial_cash") or 0),
                    "nav_dirty_from_date": row.get("nav_dirty_from_date"),
                }
            ]
            self.description = _rows_to_description(
                self._rows, ["initial_cash", "nav_dirty_from_date"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("SELECT * FROM TRADES WHERE PORTFOLIO_ID = ? ORDER BY DATE ASC"):
            self._rows = _fetch_all_rows(
                "Trades",
                "*",
                lambda q: q.eq("portfolio_id", params[0]).order("date", desc=False),
            )
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["Trades"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith(
            "SELECT DATE, STOCK_ID FROM TRADES WHERE PORTFOLIO_ID = ? ORDER BY DATE ASC"
        ):
            self._rows = _fetch_all_rows(
                "Trades",
                "date,stock_id",
                lambda q: q.eq("portfolio_id", params[0]).order("date", desc=False),
            )
            self.description = _rows_to_description(
                self._rows, ["date", "stock_id"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("SELECT * FROM CASHFLOWS WHERE PORTFOLIO_ID = ? ORDER BY DATE ASC"):
            self._rows = _fetch_all_rows(
                "CashFlows",
                "*",
                lambda q: q.eq("portfolio_id", params[0]).order("date", desc=False),
            )
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["CashFlows"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("INSERT INTO PORTFOLIOS"):
            record = {
                "name": params[0],
                "t0_cash": 0,
                "t2_cash": 0,
                "initial_cash": params[1] if len(params) > 1 else 0,
                "nav_dirty_from_date": None,
            }
            try:
                response = client.table("Portfolios").insert(record).execute()
            except Exception as exc:
                if _is_portfolio_duplicate_error(exc):
                    raise sqlite3.IntegrityError(str(exc)) from exc
                raise
            self._rows = response.data or []
            self.lastrowid = self._rows[0].get("id") if self._rows else None
            self.description = _rows_to_description(
                self._rows, SUPABASE_TABLE_COLUMNS["Portfolios"]
            )
            self.rowcount = len(self._rows)
            return self

        if sql_upper.startswith("DELETE FROM TRADES WHERE PORTFOLIO_ID"):
            client.table("Trades").delete().eq("portfolio_id", params[0]).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("DELETE FROM TRADES WHERE ID = ? AND PORTFOLIO_ID = ?"):
            client.table("Trades").delete().eq("id", params[0]).eq(
                "portfolio_id", params[1]
            ).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("DELETE FROM CASHFLOWS WHERE PORTFOLIO_ID"):
            client.table("CashFlows").delete().eq("portfolio_id", params[0]).execute()
            self.rowcount = 1
            return self

        if sql_upper.startswith("DELETE FROM PORTFOLIOS WHERE ID"):
            client.table("Portfolios").delete().eq("id", params[0]).execute()
            self.rowcount = 1
            return self

        raise NotImplementedError(
            f"這段 SQL 尚未接到 Supabase 相容層，請補轉換規則：{sql_clean}"
        )

    def _backfill_default_portfolios_initial_cash(self):
        portfolios = _fetch_all_rows(
            "Portfolios",
            "id,name,initial_cash",
            lambda q: q.in_("name", ["自己", "爸爸"]),
        )
        for portfolio in portfolios:
            if float(portfolio.get("initial_cash") or 0) != 0:
                continue
            cashflows = _fetch_all_rows(
                "CashFlows",
                "id",
                lambda q, pid=portfolio["id"]: q.eq("portfolio_id", pid),
            )
            if cashflows:
                continue
            seed_cash = 1000000.0 if portfolio.get("name") == "自己" else 500000.0
            get_supabase_client().table("Portfolios").update(
                {"initial_cash": seed_cash}
            ).eq("id", portfolio["id"]).execute()


def get_db_connection():
    return SupabaseDBConnection()
