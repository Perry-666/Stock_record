import time
from datetime import datetime

import httpx
import pandas as pd

try:
    from postgrest.exceptions import APIError
except Exception:
    APIError = None

from supabase_db import (
    SUPABASE_PAGE_SIZE,
    get_supabase_client,
)


def _is_retryable_api_error(exc):
    if APIError is None or not isinstance(exc, APIError):
        return False
    message = str(exc).lower()
    return (
        "json could not be generated" in message
        or "cloudflare" in message
        or "500 internal server error" in message
        or "'code': 500" in message
        or '"code": 500' in message
    )


def _run_with_retry(operation, retries=3, delay_seconds=0.35):
    last_exc = None
    for attempt in range(retries):
        try:
            return operation()
        except (
            httpx.RemoteProtocolError,
            httpx.ConnectError,
            httpx.ReadError,
            httpx.WriteError,
            httpx.TimeoutException,
        ) as exc:
            last_exc = exc
            if attempt >= retries - 1:
                raise
            time.sleep(delay_seconds * (attempt + 1))
        except Exception as exc:
            if not _is_retryable_api_error(exc) or attempt >= retries - 1:
                raise
            last_exc = exc
            time.sleep(delay_seconds * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    return None


def _fetch_all_rows(table_name, columns="*", query_modifier=None):
    rows = []
    start = 0

    while True:
        def _execute_page():
            query = get_supabase_client().table(table_name).select(columns)
            if query_modifier is not None:
                query = query_modifier(query)
            return query.range(start, start + SUPABASE_PAGE_SIZE - 1).execute()

        response = _run_with_retry(_execute_page)
        page_rows = response.data or []
        rows.extend(page_rows)
        if len(page_rows) < SUPABASE_PAGE_SIZE:
            break
        start += SUPABASE_PAGE_SIZE

    return rows


def _to_dataframe(rows, columns=None):
    df = pd.DataFrame(rows or [])
    if columns is not None:
        for col in columns:
            if col not in df.columns:
                df[col] = pd.Series(dtype="object")
        df = df[columns]
    return df


def _parse_columns(columns):
    if not columns or columns == "*":
        return None
    return [col.strip().strip('"') for col in str(columns).split(",") if col.strip()]


class PortfolioRepository:
    def list_portfolios(self):
        rows = _fetch_all_rows(
            "Portfolios",
            "id,name,t0_cash,t2_cash,initial_cash,nav_dirty_from_date",
            lambda q: q.order("id", desc=False),
        )
        return _to_dataframe(
            rows,
            ["id", "name", "t0_cash", "t2_cash", "initial_cash", "nav_dirty_from_date"],
        )

    def get_state(self, portfolio_id):
        rows = _fetch_all_rows(
            "Portfolios",
            "t0_cash,t2_cash",
            lambda q: q.eq("id", portfolio_id).limit(1),
        )
        if not rows:
            return {"t0_cash": 0.0, "t2_cash": 0.0}
        row = rows[0]
        return {
            "t0_cash": float(row.get("t0_cash") or 0.0),
            "t2_cash": float(row.get("t2_cash") or 0.0),
        }

    def get_initial_cash(self, portfolio_id):
        rows = _fetch_all_rows(
            "Portfolios",
            "initial_cash",
            lambda q: q.eq("id", portfolio_id).limit(1),
        )
        return float((rows[0] if rows else {}).get("initial_cash") or 0.0)

    def get_nav_dirty_from_date(self, portfolio_id):
        rows = _fetch_all_rows(
            "Portfolios",
            "nav_dirty_from_date",
            lambda q: q.eq("id", portfolio_id).limit(1),
        )
        return (rows[0] if rows else {}).get("nav_dirty_from_date")

    def set_nav_dirty_from_date(self, portfolio_id, dirty_from_date):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("Portfolios")
            .update({"nav_dirty_from_date": dirty_from_date})
            .eq("id", portfolio_id)
            .execute()
        )

    def update_cash(self, portfolio_id, t0_cash, t2_cash):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("Portfolios")
            .update({"t0_cash": float(t0_cash), "t2_cash": float(t2_cash)})
            .eq("id", portfolio_id)
            .execute()
        )

    def create_portfolio(self, name, initial_cash=0.0):
        response = _run_with_retry(
            lambda: get_supabase_client()
            .table("Portfolios")
            .insert(
                {
                    "name": name,
                    "initial_cash": float(initial_cash or 0),
                    "t0_cash": float(initial_cash or 0),
                    "t2_cash": float(initial_cash or 0),
                    "nav_dirty_from_date": None,
                }
            )
            .execute()
        )
        return int((response.data or [{}])[0].get("id"))

    def delete_portfolio(self, portfolio_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("Portfolios")
            .delete()
            .eq("id", portfolio_id)
            .execute()
        )


class TradeRepository:
    def list_trades(self, portfolio_id, columns="*"):
        rows = _fetch_all_rows(
            "Trades",
            columns,
            lambda q: q.eq("portfolio_id", portfolio_id)
            .order("date", desc=False)
            .order("id", desc=False),
        )
        return _to_dataframe(rows, _parse_columns(columns))

    def list_unsettled_trades(self, portfolio_id, is_disposed=None):
        def _modifier(query):
            query = query.eq("portfolio_id", portfolio_id).eq("settled", 0)
            if is_disposed is not None:
                query = query.eq("is_disposed", bool(is_disposed))
            return query.order("date", desc=False).order("id", desc=False)

        return _to_dataframe(
            _fetch_all_rows(
                "Trades",
                "id,date,stock_id,action,price,shares,is_disposed",
                _modifier,
            ),
            ["id", "date", "stock_id", "action", "price", "shares", "is_disposed"],
        )

    def get_trade_identity(self, portfolio_id, trade_id):
        rows = _fetch_all_rows(
            "Trades",
            "date,action",
            lambda q: q.eq("id", int(trade_id)).eq("portfolio_id", portfolio_id).limit(1),
        )
        return rows[0] if rows else None

    def insert_trade(self, record):
        payload = dict(record or {})
        try:
            response = _run_with_retry(
                lambda: get_supabase_client().table("Trades").insert(payload).execute()
            )
        except Exception as exc:
            message = str(exc).lower()
            if "take_profit_price" not in message and "stop_loss_price" not in message:
                raise
            payload.pop("take_profit_price", None)
            payload.pop("stop_loss_price", None)
            response = _run_with_retry(
                lambda: get_supabase_client().table("Trades").insert(payload).execute()
            )
        return int((response.data or [{}])[0].get("id"))

    def update_trade(self, portfolio_id, trade_id, payload):
        update_payload = dict(payload or {})
        try:
            _run_with_retry(
                lambda: get_supabase_client()
                .table("Trades")
                .update(update_payload)
                .eq("id", int(trade_id))
                .eq("portfolio_id", portfolio_id)
                .execute()
            )
        except Exception as exc:
            message = str(exc).lower()
            if "take_profit_price" not in message and "stop_loss_price" not in message:
                raise
            update_payload.pop("take_profit_price", None)
            update_payload.pop("stop_loss_price", None)
            _run_with_retry(
                lambda: get_supabase_client()
                .table("Trades")
                .update(update_payload)
                .eq("id", int(trade_id))
                .eq("portfolio_id", portfolio_id)
                .execute()
            )

    def delete_trade(self, portfolio_id, trade_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("Trades")
            .delete()
            .eq("id", int(trade_id))
            .eq("portfolio_id", portfolio_id)
            .execute()
        )

    def mark_trade_settled(self, trade_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("Trades")
            .update({"settled": 1})
            .eq("id", int(trade_id))
            .execute()
        )

    def delete_portfolio_trades(self, portfolio_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("Trades")
            .delete()
            .eq("portfolio_id", portfolio_id)
            .execute()
        )


class CashFlowRepository:
    def list_cashflows(self, portfolio_id, columns="*"):
        rows = _fetch_all_rows(
            "CashFlows",
            columns,
            lambda q: q.eq("portfolio_id", portfolio_id).order("date", desc=False),
        )
        return _to_dataframe(rows, _parse_columns(columns))

    def insert_cashflow(self, portfolio_id, date, cf_type, amount):
        response = _run_with_retry(
            lambda: get_supabase_client()
            .table("CashFlows")
            .insert(
                {
                    "portfolio_id": portfolio_id,
                    "date": date,
                    "type": cf_type,
                    "amount": float(amount or 0),
                }
            )
            .execute()
        )
        return int((response.data or [{}])[0].get("id"))

    def delete_portfolio_cashflows(self, portfolio_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("CashFlows")
            .delete()
            .eq("portfolio_id", portfolio_id)
            .execute()
        )


class MarketHolidayRepository:
    def list_holidays(self):
        return _to_dataframe(
            _fetch_all_rows(
                "MarketHolidays",
                "date,reason,is_settlement_open",
                lambda q: q.order("date", desc=False),
            ),
            ["date", "reason", "is_settlement_open"],
        )

    def upsert_holiday(self, date_str, reason="", is_settlement_open=False):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("MarketHolidays")
            .upsert(
                {
                    "date": str(date_str),
                    "reason": str(reason or ""),
                    "is_settlement_open": bool(is_settlement_open),
                },
                on_conflict="date",
            )
            .execute()
        )

    def upsert_many(self, rows):
        if not rows:
            return 0
        payload = [
            {
                "date": row["date"],
                "reason": row.get("reason", "") or "",
                "is_settlement_open": bool(row.get("is_settlement_open", False)),
            }
            for row in rows
        ]
        _run_with_retry(
            lambda: get_supabase_client()
            .table("MarketHolidays")
            .upsert(payload, on_conflict="date")
            .execute()
        )
        return len(payload)

    def delete_holiday(self, date_str):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("MarketHolidays")
            .delete()
            .eq("date", str(date_str))
            .execute()
        )


class StockNameRepository:
    def list_stock_names(self, stock_ids):
        normalized_ids = [str(s).strip() for s in stock_ids if str(s).strip()]
        if not normalized_ids:
            return []
        return _fetch_all_rows(
            "StockNames",
            "stock_id,name,full_symbol",
            lambda q: q.in_("stock_id", normalized_ids),
        )

    def get_full_symbol(self, stock_id):
        rows = _fetch_all_rows(
            "StockNames",
            "full_symbol",
            lambda q: q.eq("stock_id", stock_id).limit(1),
        )
        return (rows[0] if rows else {}).get("full_symbol")

    def update_full_symbol(self, stock_id, full_symbol):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("StockNames")
            .update({"full_symbol": full_symbol})
            .eq("stock_id", stock_id)
            .execute()
        )

    def upsert_stock_name(self, stock_id, name, full_symbol):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("StockNames")
            .upsert(
                {
                    "stock_id": stock_id,
                    "name": name,
                    "full_symbol": full_symbol,
                },
                on_conflict="stock_id",
            )
            .execute()
        )


class ManualPriceOverrideRepository:
    def get_overrides(self, stock_ids=None):
        if stock_ids:
            rows = _fetch_all_rows(
                "ManualPriceOverrides",
                "stock_id,price",
                lambda q: q.in_("stock_id", stock_ids),
            )
        else:
            rows = _fetch_all_rows("ManualPriceOverrides", "stock_id,price")
        return {row.get("stock_id"): float(row.get("price") or 0) for row in rows}

    def set_override(self, stock_id, price):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("ManualPriceOverrides")
            .upsert(
                {
                    "stock_id": stock_id,
                    "price": float(price),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                on_conflict="stock_id",
            )
            .execute()
        )

    def delete_override(self, stock_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("ManualPriceOverrides")
            .delete()
            .eq("stock_id", stock_id)
            .execute()
        )


class PriceSnapshotRepository:
    def upsert_snapshots(self, records):
        if not records:
            return 0
        payload_map = {}
        for record in records:
            payload_map[(record["date"], record["stock_id"])] = record
        payload = list(payload_map.values())
        _run_with_retry(
            lambda: get_supabase_client()
            .table("PriceSnapshots")
            .upsert(payload, on_conflict="date,stock_id")
            .execute()
        )
        return len(payload)

    def get_snapshot_rows(self, stock_ids, start_date=None, end_date=None):
        def _modifier(query):
            query = query.in_("stock_id", stock_ids)
            if start_date:
                query = query.gte("date", start_date)
            if end_date:
                query = query.lte("date", end_date)
            return query.order("date", desc=False)

        return _fetch_all_rows(
            "PriceSnapshots",
            "date,stock_id,close_price",
            _modifier,
        )

    def get_min_latest_date(self, stock_ids):
        rows = _fetch_all_rows(
            "PriceSnapshots",
            "stock_id,date",
            lambda q: q.in_("stock_id", stock_ids),
        )
        df = pd.DataFrame(rows or [])
        if df.empty or df["stock_id"].nunique() < len(set(stock_ids)):
            return None
        latest_date = pd.to_datetime(df.groupby("stock_id")["date"].max(), errors="coerce").min()
        if pd.isna(latest_date):
            return None
        return latest_date.strftime("%Y-%m-%d")

    def get_snapshot_status(self, stock_ids):
        rows = _fetch_all_rows(
            "PriceSnapshots",
            "date,stock_id",
            lambda q: q.in_("stock_id", stock_ids),
        )
        df = pd.DataFrame(rows or [])
        if df.empty:
            return {"dates": 0, "latest_date": None}
        return {
            "dates": int(df["date"].nunique()),
            "latest_date": str(df["date"].max()),
        }


class DailyNavSnapshotRepository:
    def list_snapshots(self, portfolio_id):
        rows = _fetch_all_rows(
            "DailyNavSnapshots",
            (
                "date,nav,cash_flow,cash,holdings_value,daily_return,twr,"
                "benchmark_price,notes,is_trading_day,benchmark_daily_return,"
                "benchmark_cum_return,drawdown,daily_alpha,cum_alpha,"
                "allocation_json,win_flag"
            ),
            lambda q: q.eq("portfolio_id", portfolio_id).order("date", desc=False),
        )
        return _to_dataframe(rows)

    def upsert_snapshots(self, portfolio_id, records):
        if not records:
            return 0
        payload_map = {
            (portfolio_id, record["date"]): {
                "portfolio_id": portfolio_id,
                **record,
            }
            for record in records
        }
        payload = list(payload_map.values())
        _run_with_retry(
            lambda: get_supabase_client()
            .table("DailyNavSnapshots")
            .upsert(payload, on_conflict="portfolio_id,date")
            .execute()
        )
        return len(payload)

    def delete_from_date(self, portfolio_id, from_date):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("DailyNavSnapshots")
            .delete()
            .eq("portfolio_id", portfolio_id)
            .gte("date", from_date)
            .execute()
        )

    def delete_portfolio_snapshots(self, portfolio_id):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("DailyNavSnapshots")
            .delete()
            .eq("portfolio_id", portfolio_id)
            .execute()
        )


class MacroJournalRepository:
    def list_journals(self):
        return _to_dataframe(
            _fetch_all_rows(
                "MacroJournals",
                "id,date,content",
                lambda q: q.order("date", desc=True),
            ),
            ["id", "date", "content"],
        )

    def upsert_journal(self, date, content):
        _run_with_retry(
            lambda: get_supabase_client()
            .table("MacroJournals")
            .upsert({"date": date, "content": content}, on_conflict="date")
            .execute()
        )


class TradeCycleReviewRepository:
    def get_review(self, portfolio_id, stock_id, cycle_no):
        try:
            rows = _fetch_all_rows(
                "TradeCycleReviews",
                "review_text,updated_at",
                lambda q: q.eq("portfolio_id", portfolio_id)
                .eq("stock_id", stock_id)
                .eq("cycle_no", int(cycle_no))
                .limit(1),
            )
        except Exception:
            return None
        if not rows:
            return None
        return (rows[0] or {}).get("review_text")

    def upsert_review(self, portfolio_id, stock_id, cycle_no, review_text):
        try:
            _run_with_retry(
                lambda: get_supabase_client()
                .table("TradeCycleReviews")
                .upsert(
                    {
                        "portfolio_id": portfolio_id,
                        "stock_id": stock_id,
                        "cycle_no": int(cycle_no),
                        "review_text": str(review_text or ""),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    on_conflict="portfolio_id,stock_id,cycle_no",
                )
                .execute()
            )
            return True
        except Exception:
            return False

    def delete_portfolio_reviews(self, portfolio_id):
        try:
            _run_with_retry(
                lambda: get_supabase_client()
                .table("TradeCycleReviews")
                .delete()
                .eq("portfolio_id", portfolio_id)
                .execute()
            )
        except Exception:
            return


portfolio_repository = PortfolioRepository()
trade_repository = TradeRepository()
cashflow_repository = CashFlowRepository()
market_holiday_repository = MarketHolidayRepository()
stock_name_repository = StockNameRepository()
manual_price_override_repository = ManualPriceOverrideRepository()
price_snapshot_repository = PriceSnapshotRepository()
daily_nav_snapshot_repository = DailyNavSnapshotRepository()
macro_journal_repository = MacroJournalRepository()
trade_cycle_review_repository = TradeCycleReviewRepository()
