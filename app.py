import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import yfinance as yf
import urllib.request
import requests
import re
import json
import os
import html
from dotenv import load_dotenv

load_dotenv()  # 載入 .env 檔案

DB_NAME = "trading_system.db"
_session = requests.Session()
_session.headers.update(
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
)


def normalize_stock_id(s):
    s = str(s).strip().upper()
    if s.endswith(".TW"):
        s = s[:-3]
    if s.endswith(".TWO"):
        s = s[:-4]
    # Strip any appended names (e.g., "2330 台積電" -> "2330")
    s = re.split(r"[\s\-]", s)[0].strip()
    return s


# --- DB Helpers ---
def get_db_connection():
    conn = sqlite3.connect(DB_NAME, timeout=30.0, check_same_thread=False)
    # 啟用 Write-Ahead Logging 模式，大幅減少讀寫鎖衝突 (database is locked)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except sqlite3.OperationalError:
        pass
    conn.row_factory = sqlite3.Row
    return conn


def ensure_db_schema():
    """補齊舊版資料庫缺少的表與欄位，避免新版功能直接報錯。"""
    conn = get_db_connection()
    try:
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

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS StockNames (
                stock_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                full_symbol TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_portfolios():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM Portfolios", conn)
    conn.close()
    return df


def get_portfolio_state(portfolio_id):
    conn = get_db_connection()
    state = conn.execute(
        "SELECT t0_cash, t2_cash FROM Portfolios WHERE id = ?", (portfolio_id,)
    ).fetchone()
    conn.close()
    return state["t0_cash"], state["t2_cash"]


def get_inventory(portfolio_id, stock_id=None):
    conn = get_db_connection()
    query = """
        SELECT stock_id, SUM(
            CASE 
                WHEN action IN ('Buy', 'Add', 'Setup') THEN shares
                WHEN action IN ('Reduce', 'Close') THEN -shares
                ELSE 0
            END
        ) as current_shares
        FROM Trades
        WHERE portfolio_id = ?
    """
    params = [portfolio_id]
    if stock_id:
        query += " AND stock_id = ?"
        params.append(stock_id)

    query += " GROUP BY stock_id HAVING current_shares > 0"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


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
):
    conn = get_db_connection()
    # 判斷是否為「立即交割」(T+0)：處置股或期初建檔
    # 在台灣，處置股通常圈存，所以 T+0 = T+2 都會變動
    is_settled = 1 if (is_disposed or action == "Setup") else 0
    
    stock_id = normalize_stock_id(stock_id)
    conn.execute(
        """
        INSERT INTO Trades (portfolio_id, date, stock_id, action, price, shares, is_disposed, basic_check, tech_check, chips_check, industry_check, trading_notes, settled)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            portfolio_id,
            date,
            stock_id,
            action,
            price,
            shares,
            int(is_disposed),
            int(basic),
            int(tech),
            int(chips),
            int(indus),
            notes,
            is_settled
        ),
    )
    conn.commit()
    conn.close()
    
    # 交易完成後，強制重算該資金池的所有現金狀態
    recalculate_portfolio_cash(portfolio_id)

def recalculate_portfolio_cash(portfolio_id):
    """ 從頭掃描所有紀錄，精準校正 T+0 (可用資金) 與 T+2 (銀行結餘) """
    conn = get_db_connection()
    # 1. 外部資金流 (入/出金/配息) - 同步影響 T+0 與 T+2
    cf = conn.execute("SELECT type, amount FROM CashFlows WHERE portfolio_id=?", (portfolio_id,)).fetchall()
    total_cf = sum([c["amount"] if c["type"] != "Withdrawal" else -c["amount"] for c in cf])
    
    # 2. 交易流 T+0 (所有已成交的買賣)
    tr_all = conn.execute("SELECT action, price, shares FROM Trades WHERE portfolio_id=?", (portfolio_id,)).fetchall()
    t0_tr_flow = sum([-t["price"]*t["shares"] if t["action"] in ["Buy", "Add"] else t["price"]*t["shares"] for t in tr_all if t["action"] != "Setup"])
    
    # 3. 交易流 T+2 (已交割的買賣)
    tr_settled = conn.execute("SELECT action, price, shares FROM Trades WHERE portfolio_id=? AND settled=1", (portfolio_id,)).fetchall()
    t2_tr_flow = sum([-t["price"]*t["shares"] if t["action"] in ["Buy", "Add"] else t["price"]*t["shares"] for t in tr_settled if t["action"] != "Setup"])
    
    new_t0 = total_cf + t0_tr_flow
    new_t2 = total_cf + t2_tr_flow
    
    conn.execute("UPDATE Portfolios SET t0_cash = ?, t2_cash = ? WHERE id = ?", (new_t0, new_t2, portfolio_id))
    conn.commit()
    conn.close()

def auto_process_settlement(portfolio_id):
    """ 檢查日期，將滿兩天的交易標記為已交割並重算現金 """
    conn = get_db_connection()
    # 簡單模擬 T+2： 只要日期 <= 2天前，就標記為已交割
    # (更嚴格可考慮加入週末判斷，但這邊以 date 比較為準)
    changed = conn.execute("""
        UPDATE Trades SET settled = 1 
        WHERE portfolio_id = ? AND settled = 0 AND is_disposed = 0
        AND date <= date('now', '-2 days')
    """, (portfolio_id,)).rowcount
    
    conn.commit()
    conn.close()
    
    if changed > 0:
        recalculate_portfolio_cash(portfolio_id)


def execute_cashflow(portfolio_id, date, cf_type, amount):
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO CashFlows (portfolio_id, date, type, amount) VALUES (?, ?, ?, ?)",
        (portfolio_id, date, cf_type, amount),
    )
    conn.commit()
    conn.close()
    recalculate_portfolio_cash(portfolio_id)


def get_macro_journals():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM MacroJournals ORDER BY date DESC", conn)
    conn.close()
    return df


def save_macro_journal(date, content):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO MacroJournals (date, content) VALUES (?, ?)",
        (date, content),
    )
    conn.commit()
    conn.close()


def get_symbol_candidates(sid):
    sid = normalize_stock_id(sid)
    candidates = []

    conn = get_db_connection()
    row = conn.execute(
        "SELECT full_symbol FROM StockNames WHERE stock_id = ?", (sid,)
    ).fetchone()
    conn.close()

    if row and row["full_symbol"]:
        candidates.append(row["full_symbol"])

    candidates.extend([f"{sid}.TW", f"{sid}.TWO"])
    return list(dict.fromkeys(candidates))


def update_stock_full_symbol(stock_id, full_symbol):
    stock_id = normalize_stock_id(stock_id)
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE StockNames SET full_symbol = ? WHERE stock_id = ?",
            (full_symbol, stock_id),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_first_valid_history(stock_id, start_date=None, period="1mo", auto_adjust=True):
    """依序嘗試 .TW / .TWO，取第一個有資料的 ticker。"""
    end_d = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    for candidate in get_symbol_candidates(stock_id):
        try:
            t_obj = yf.Ticker(candidate)
            hist = t_obj.history(
                period=period if period else "1mo",
                start=start_date,
                end=end_d,
                auto_adjust=auto_adjust,
            )
            if not hist.empty:
                update_stock_full_symbol(stock_id, candidate)
                return candidate, hist
        except Exception:
            continue

    return None, pd.DataFrame()


def get_full_yf_symbol(sid):
    """ 從資料庫獲取全名符號 (.TW / .TWO)，若無則調用爬蟲確認 """
    sid = normalize_stock_id(sid)
    conn = get_db_connection()
    row = conn.execute("SELECT full_symbol FROM StockNames WHERE stock_id = ?", (sid,)).fetchone()
    conn.close()
    
    if row and row["full_symbol"]:
        return row["full_symbol"]
    
    # 若資料庫無此代號，強制跑一次爬蟲來確認
    # 此處不直接改資料庫，而是回傳爬蟲結果 (DB 會在 get_stock_display_names 中被更新，但這裡先防呆)
    try:
        url = f"https://tw.stock.yahoo.com/quote/{sid}"
        r = _session.get(url, timeout=3)
        if r.status_code == 200:
            full = f"{sid}.TWO" if "上櫃" in r.text else f"{sid}.TW"
            return full
    except:
        pass

    resolved_symbol, _ = fetch_first_valid_history(sid, period="5d", auto_adjust=True)
    if resolved_symbol:
        return resolved_symbol

    return sid + ".TW"

@st.cache_data(ttl=600)  # 縮短快取時間到 10 分鐘，確保現價及時更新
def fetch_yfinance_history(tickers, start_date=None, period="1mo"):
    if not tickers:
        return pd.DataFrame(), pd.DataFrame(), {}

    results_close = {}
    results_divs = {}
    symbol_map = {}
    sids = list(dict.fromkeys(normalize_stock_id(t) for t in tickers))

    # 由於 MultiIndex 下載容易在最新一天產生跨股票的 NaN 填充問題，
    # 我們改用個別/小批量下載，或是使用更能容錯的方式
    # 這裡我們使用個別下載並合併，以確保每一支股票都能拿到它最新的 row
    def get_one_ticker(stock_id):
        resolved_symbol, hist = fetch_first_valid_history(
            stock_id, start_date=start_date, period=period, auto_adjust=True
        )
        if hist.empty:
            return stock_id, get_full_yf_symbol(stock_id), pd.Series(dtype=float), pd.Series(dtype=float)

        close_series = hist["Close"].ffill()
        div_series = (
            hist["Dividends"] if "Dividends" in hist else pd.Series(0.0, index=hist.index)
        )
        return stock_id, resolved_symbol, close_series, div_series

    # 平行下載提升速度
    with ThreadPoolExecutor(max_workers=5) as executor:
        merged_res = list(executor.map(get_one_ticker, sids))

    for sid, resolved_symbol, close_series, div_series in merged_res:
        symbol_map[sid] = resolved_symbol
        results_close[sid] = close_series
        results_divs[sid] = div_series

    benchmark_symbol, benchmark_hist = fetch_first_valid_history(
        "0050", start_date=start_date, period=period, auto_adjust=True
    )
    if not benchmark_hist.empty:
        results_close["0050.TW"] = benchmark_hist["Close"].ffill()
        results_divs["0050.TW"] = (
            benchmark_hist["Dividends"]
            if "Dividends" in benchmark_hist
            else pd.Series(0.0, index=benchmark_hist.index)
        )
    elif "0050.TW" not in results_close:
        results_close["0050.TW"] = pd.Series(dtype=float)
        results_divs["0050.TW"] = pd.Series(dtype=float)

    if benchmark_symbol:
        symbol_map["0050"] = benchmark_symbol

    # 合併成 DataFrame
    close_df = pd.DataFrame(results_close).ffill()
    divs_df = pd.DataFrame(results_divs).fillna(0.0)
    
    return close_df, divs_df, symbol_map


from concurrent.futures import ThreadPoolExecutor

@st.cache_data(ttl=86400)
def get_stock_display_names(tickers):
    if not tickers:
        return {}

    conn = get_db_connection()
    existing_map = {}
    existing_full = {}
    try:
        sids = [normalize_stock_id(t) for t in tickers]
        t_list = "('" + "','".join(sids) + "')"
        rows = conn.execute(f"SELECT stock_id, name, full_symbol FROM StockNames WHERE stock_id IN {t_list}").fetchall()
        for r in rows:
            existing_map[r["stock_id"]] = r["name"]
            existing_full[r["stock_id"]] = r["full_symbol"]
    except:
        pass
    conn.close()
    
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

    # 封裝爬蟲邏輯供平行處理
    def scrape_one(t_in):
        code = normalize_stock_id(t_in)
        name, full = None, None
        try:
            url = f"https://tw.stock.yahoo.com/quote/{code}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            r = _session.get(url, timeout=3)
            if r.status_code == 200:
                html = r.text
                match = re.search(r"<title>([^<]+?)\s*\(", html)
                if match:
                    name = match.group(1).strip()
                full = f"{code}.TWO" if "上櫃" in html else f"{code}.TW"
        except:
            pass
            
        if not name:
            try:
                tkr = yf.Ticker(code + ".TW")
                name = tkr.info.get("shortName")
                full = code + ".TW"
            except:
                pass
        return t_in, code, name, full

    # 平行爬取以解決超市問題
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(scrape_one, to_scrape))

    # 批次存回資料庫
    conn = get_db_connection()
    for t_in, code, name, full in results:
        if name:
            res[t_in] = f"{code} {name}"
            try:
                conn.execute("INSERT OR REPLACE INTO StockNames (stock_id, name, full_symbol) VALUES (?, ?, ?)", 
                             (code, name, full))
            except: pass
        else:
            res[t_in] = t_in
    conn.commit()
    conn.close()
    return res


def calculate_twr_and_nav(portfolio_id):
    conn = get_db_connection()
    trades_df = pd.read_sql_query(
        "SELECT * FROM Trades WHERE portfolio_id = ? ORDER BY date ASC",
        conn,
        params=(portfolio_id,),
    )
    cf_df = pd.read_sql_query(
        "SELECT * FROM CashFlows WHERE portfolio_id = ? ORDER BY date ASC",
        conn,
        params=(portfolio_id,),
    )
    conn.close()

    if not trades_df.empty:
        trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)

    if trades_df.empty and cf_df.empty:
        return pd.DataFrame(), 0, 0

    dates = []
    if not trades_df.empty:
        dates.extend(trades_df["date"].tolist())
    if not cf_df.empty:
        dates.extend(cf_df["date"].tolist())

    start_date_str = min(dates)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.now()
    date_range = [d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, end_date)]

    all_tickers = trades_df["stock_id"].unique().tolist() if not trades_df.empty else []
    price_data, divs_data, symbol_map = fetch_yfinance_history(
        all_tickers, start_date_str
    )

    history = []

    # 建構逐日事件字典
    events_by_date = {}
    for d in date_range:
        events_by_date[d] = {"trades": [], "cf": []}

    for idx, r in trades_df.iterrows():
        events_by_date[r["date"]]["trades"].append(r)
    for idx, r in cf_df.iterrows():
        events_by_date[r["date"]]["cf"].append(r)

    trade_price_fallback = {}
    if not trades_df.empty:
        for stock_id, group in trades_df.groupby("stock_id"):
            trade_price_fallback[stock_id] = group[["date", "price"]].sort_values("date")

    current_cash = 0.0
    current_inv = {}
    last_nav = 0.0
    cum_twr = 1.0

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
        for s_id, sh in current_inv.items():
            if sh > 0:
                yf_code = symbol_map.get(s_id)

                if not divs_data.empty and yf_code in divs_data.columns:
                    if d in divs_data.index.strftime("%Y-%m-%d"):
                        div_v = divs_data[yf_code].loc[d]
                        if isinstance(div_v, pd.Series):
                            div_v = div_v.iloc[0]
                        if pd.notna(div_v) and div_v > 0:
                            # 股票除息！將配息直接納入現金！(外部現金流不變，因為這是資本池內部轉換)
                            current_cash += float(div_v) * sh

                price = np.nan
                if not price_data.empty and yf_code in price_data.columns:
                    try:
                        # 最佳化：直接抓取對應日期的價格
                        price = price_data.at[d, yf_code]
                        if pd.isna(price):
                            # 如果當天是假日或是沒抓到，改用 ffill 過後的最近一筆價格
                            # 由於 price_data 已經做過 ffill()，loc[:d] 的最後一筆通常就是最近期價格
                            price = price_data[yf_code].loc[:d].iloc[-1]
                    except:
                        price = np.nan

                if pd.isna(price) and s_id in trade_price_fallback:
                    fallback_df = trade_price_fallback[s_id]
                    past_prices = fallback_df[fallback_df["date"] <= d]
                    if not past_prices.empty:
                        price = float(past_prices.iloc[-1]["price"])

                if not pd.isna(price):
                    inv_value += float(price) * sh

        daily_nav = current_cash + inv_value

        daily_return = 0.0
        if last_nav > 0:
            daily_return = (daily_nav - daily_ext_cf - last_nav) / last_nav

        cum_twr = cum_twr * (1 + daily_return)
        last_nav = daily_nav

        b_price = 100.0
        if not price_data.empty and "0050.TW" in price_data.columns:
            sub_data = price_data.loc[:d]
            if not sub_data.empty and not pd.isna(sub_data["0050.TW"].iloc[-1]):
                b_price = sub_data["0050.TW"].iloc[-1]

        # 過濾圖表日期: 確保橫軸是交易日，只有當那天是交易日，或者是我們有建檔異動時才納入
        # is_trading_day 代表有 0050 的當日報價 (不是用 forward fill 推導出來的結果)
        is_trading_day = False
        if not price_data.empty:
            if d in price_data.index.strftime("%Y-%m-%d"):
                is_trading_day = True

        # 只要是交易日，或是發生了操作(出入金/交易)，我們就紀錄下來畫點
        if is_trading_day or has_events or d == start_date_str:
            # Extract Trade Summaries
            trade_strs = []
            for tr in events_by_date[d]["trades"]:
                trade_strs.append(f"{tr['action']} {tr['stock_id']} {tr['shares']}股")
            trade_summary = ", ".join(trade_strs) if trade_strs else ""

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

        # Drawdown computation
        twr_index = df_hist["TWR"] / 100.0 + 1.0
        rolling_max = twr_index.cummax()
        # prevent division by zero gracefully
        df_hist["Drawdown"] = np.where(
            rolling_max > 0, (twr_index - rolling_max) / rolling_max * 100, 0.0
        )

        # Alpha computation
        df_hist["DailyAlpha"] = df_hist["DailyReturn"] - df_hist["BenchmarkDailyReturn"]
        df_hist["CumAlpha"] = df_hist["TWR"] - df_hist["BenchmarkCumReturn"]
        df_hist["Win?"] = df_hist["DailyAlpha"].apply(
            lambda x: "Y" if x > 0 else ("N" if x < 0 else "-")
        )

    return (
        df_hist,
        df_hist.iloc[-1]["NAV"] if not df_hist.empty else 0,
        (cum_twr - 1.0) * 100,
    )


# --- Journal logic ---
@st.cache_data(ttl=86400)
def get_stock_dividends(stock_id, start_date):
    # 使用 helper 確保符號正確 (.TW/.TWO)
    yf_code = get_full_yf_symbol(stock_id)
    try:
        # 下載至最新日期 (明天) 確保資料最全
        end_date_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        data = yf.download(
            yf_code, 
            start=start_date, 
            end=end_date_str,
            auto_adjust=False, 
            actions=True, 
            progress=False
        )
        if "Dividends" in data:
            d = data["Dividends"]
            if isinstance(d, pd.DataFrame):
                d = d.iloc[:, 0]
            d = d[d > 0].dropna()
            return d
    except:
        pass
    return pd.Series(dtype=float)


def calculate_trade_journal(trades_df):
    if trades_df.empty:
        return pd.DataFrame()

    s_id = trades_df.iloc[0]["stock_id"]
    from_date = trades_df["date"].min()

    divs = get_stock_dividends(s_id, from_date)
    trades_df = trades_df.copy()

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
                pd.concat([trades_df, pd.DataFrame(div_rows)])
                .sort_values("date")
                .reset_index(drop=True)
            )

    records = []
    avg_cost = 0.0
    accum_shares = 0
    cum_pnl = 0.0

    for idx, row in trades_df.iterrows():
        action = row["action"]
        shares = row["shares"]
        price = row["price"]

        pnl_this_trade = 0.0

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
        r_dict["avg_cost"] = avg_cost
        r_dict["accum_shares"] = accum_shares
        r_dict["realized_pnl"] = pnl_this_trade
        r_dict["cum_pnl"] = cum_pnl
        records.append(r_dict)

    return pd.DataFrame(records)


def get_holdings_detail(portfolio_id):
    conn = get_db_connection()
    t_df = pd.read_sql_query(
        "SELECT * FROM Trades WHERE portfolio_id = ? ORDER BY date ASC",
        conn,
        params=(portfolio_id,),
    )
    conn.close()

    if t_df.empty:
        return pd.DataFrame(), []

    t_df["stock_id"] = t_df["stock_id"].apply(normalize_stock_id)
    stocks = t_df["stock_id"].unique()
    active_holdings = []

    for s in stocks:
        sub_df = t_df[t_df["stock_id"] == s].copy()
        p_df = calculate_trade_journal(sub_df)
        last_row = p_df.iloc[-1]
        if last_row["accum_shares"] > 0:
            active_holdings.append(
                {
                    "Stock": s,
                    "Shares": last_row["accum_shares"],
                    "AvgCost": last_row["avg_cost"],
                    "LastTradePrice": float(sub_df.iloc[-1]["price"]),
                }
            )

    if not active_holdings:
        return pd.DataFrame(), []

    tickers = [h["Stock"] for h in active_holdings]
    price_df, divs_data, symbol_map = fetch_yfinance_history(
        tickers, period="5d"
    )
    display_names = get_stock_display_names(tickers)

    # 並行獲取庫存現價 (強健邏輯)
    results = []
    for h in active_holdings:
        s = h["Stock"]
        sid = normalize_stock_id(s)
        disp_name = display_names.get(sid, sid)
        
        current_price = 0.0
        # 如果能在剛才的批次下載中找到價格，優先使用
        if not price_df.empty and sid in price_df.columns:
            # 遍歷找到最後一個有效的價格 (處理各標的結算時間差)
            valid_p = price_df[sid].dropna()
            if not valid_p.empty:
                current_price = float(valid_p.iloc[-1])

        # 如果批次抓取還是找不到 (例如 3037 同步問題)，我們針對該檔進行單獨重試
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

        price_source = "即時報價"
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
                unrealized_pnl_pct = (
                    (unrealized_pnl / total_cost * 100) if total_cost > 0 else 0
                )
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
                "平均成本": round(h["AvgCost"], 1),
                "當前現價": final_p_display,
                "報價來源": price_source,
                "總市值": round(market_value, 1),
                "未實現損益 ($)": round(unrealized_pnl, 1),
                "未實現報酬率 (%)": unrealized_pnl_pct,
            }
        )

    return pd.DataFrame(results), active_holdings


# --- Gemini Vision AI ---
# 備用模型清單（依優先順序，各有獨立配額）
GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.0-flash-lite", "gemini-2.0-flash"]


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
            else:
                last_error = err_str
                break
    return None, last_error


def ai_vision_single_trade(image_bytes):
    if _get_gemini_client() is None:
        return [{"stock_id": "2308", "price": 1375.0, "shares": 25, "_mock": True}]

    prompt = """你是一個台灣股票交易資料擷取助手。
請仔細閱讀圖片中的交易紀錄（可能是券商 App 或對帳單截圖），擷取所有交易資訊：
- stock_id: 純數字股票代號（例如 2330，不含 .TW 或中文名稱）
- price: 成交均價（數字，保留小數點後最多2位）
- shares: 成交股數（整數）

如果圖片中有多筆交易，請回傳 JSON 陣列；如果只有一筆，回傳 JSON 物件或陣列皆可。
請嚴格回傳 JSON 格式，不要加任何說明文字。
範例格式：
[{"stock_id": "2330", "price": 750.0, "shares": 1000}, ...]"""

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
        
        # 統一轉換為 list
        if isinstance(parsed, dict):
            parsed_list = [parsed]
        elif isinstance(parsed, list):
            parsed_list = parsed
        else:
            return None

        for item in parsed_list:
            item["stock_id"] = normalize_stock_id(str(item.get("stock_id", "")))
        
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
            raw = str(item.get("股票代碼 (純代號)", ""))
            item["股票代碼 (純代號)"] = normalize_stock_id(raw)
        return parsed_list
    except Exception:
        st.error("⚠️ AI 回傳格式無法解析，請稍後再試或手動填寫。")
        return None

def process_trade_derivation(portfolio_id, parsed_data):
    stock_id = parsed_data["stock_id"]
    inv_df = get_inventory(portfolio_id, stock_id)
    current_shares = 0
    if not inv_df.empty:
        current_shares = inv_df.iloc[0]["current_shares"]
    if current_shares == 0:
        return "Buy"
    return "Add"


TRADE_NOTES_TEMPLATE = """【產業/題材邏輯】：

【技術面/進場位階】：

【籌碼面動向】：

【交易心得】：
"""

TRADE_DECISION_REMINDER_MD = """
<div class="trade-reminder-box">
  <div class="reminder-title">多因子分析提醒</div>
  <div class="reminder-row"><b>籌碼面提醒</b>：投信、外資、融資清洗、短/長線籌碼集中度、大戶持股、分點成本、特殊分點、法人買超佔比、隔日沖分點佔比、借券賣出餘額</div>
  <div class="reminder-row"><b>技術面提醒</b>：箱型盤整、支撐位/突破位、大量長紅、量縮止穩、MACD、布林通道、均線位置與斜率、跳空缺口位置、價量背離</div>
</div>
"""

TRADE_RISK_META_PREFIX = "[[TRADE_RISK]]"


def strip_trade_risk_metadata(notes_text):
    return "\n".join(
        line
        for line in str(notes_text or "").splitlines()
        if not line.strip().startswith(TRADE_RISK_META_PREFIX)
    ).strip()


def extract_trade_risk_targets_from_notes(notes_text):
    for raw_line in str(notes_text or "").splitlines():
        line_text = raw_line.strip()
        if not line_text.startswith(TRADE_RISK_META_PREFIX):
            continue
        try:
            payload = json.loads(line_text[len(TRADE_RISK_META_PREFIX):].strip())
        except Exception:
            return None, None
        return payload.get("take_profit_price"), payload.get("stop_loss_price")
    return None, None


def render_trade_decision_reminder():
    st.markdown(TRADE_DECISION_REMINDER_MD, unsafe_allow_html=True)


def render_trade_score_inputs(key_prefix, default_scores=(3, 3, 3)):
    score_cols = st.columns(3)
    technical_score = score_cols[0].slider(
        "技術面評分",
        min_value=1,
        max_value=5,
        value=int(default_scores[0]),
        key=f"{key_prefix}_technical_score",
    )
    chip_score = score_cols[1].slider(
        "籌碼面評分",
        min_value=1,
        max_value=5,
        value=int(default_scores[1]),
        key=f"{key_prefix}_chip_score",
    )
    theme_score = score_cols[2].slider(
        "題材面評分",
        min_value=1,
        max_value=5,
        value=int(default_scores[2]),
        key=f"{key_prefix}_theme_score",
    )
    return technical_score, chip_score, theme_score


def _normalize_price_target_input(price_value, target_value, mode, is_take_profit=True):
    base_price = float(price_value or 0)
    raw_target = float(target_value or 0)
    if mode == "不設定" or raw_target <= 0:
        return None
    if mode == "價格":
        return round(raw_target, 4)
    if base_price <= 0:
        return None
    if is_take_profit:
        return round(base_price * (1 + raw_target / 100.0), 4)
    return round(base_price * (1 - raw_target / 100.0), 4)


def _is_price_target_changed(old_value, new_value):
    old_target = pd.to_numeric(old_value, errors="coerce")
    new_target = pd.to_numeric(new_value, errors="coerce")
    old_target = None if pd.isna(old_target) or float(old_target) <= 0 else round(float(old_target), 4)
    new_target = None if pd.isna(new_target) or float(new_target) <= 0 else round(float(new_target), 4)
    return old_target != new_target


def render_trade_risk_plan_inputs(
    key_prefix,
    trade_price,
    default_take_profit_price=None,
    default_stop_loss_price=None,
    trade_action=None,
):
    if str(trade_action or "").strip() == "Close":
        st.caption("Close 為全數出場，不需要再填停利 / 停損。")
        return None, None

    st.markdown("##### 停利 / 停損設定")
    risk_cols = st.columns([1.05, 1, 1.05, 1])
    tp_default = float(default_take_profit_price or 0)
    sl_default = float(default_stop_loss_price or 0)
    trade_price = float(trade_price or 0)

    tp_mode = risk_cols[0].selectbox(
        "停利輸入方式",
        ["不設定", "價格", "百分比"],
        index=1 if tp_default > 0 else 0,
        key=f"{key_prefix}_take_profit_mode",
    )
    tp_default_pct = (
        max(0.0, (tp_default / trade_price - 1) * 100)
        if tp_default > 0 and trade_price > 0
        else 10.0
    )
    tp_value = 0.0
    if tp_mode != "不設定":
        tp_value = risk_cols[1].number_input(
            "停利價格 / %",
            min_value=0.0,
            value=tp_default if tp_mode == "價格" else tp_default_pct,
            step=0.5,
            key=f"{key_prefix}_take_profit_value_{tp_mode}",
        )
    else:
        risk_cols[1].caption("未設定")

    sl_mode = risk_cols[2].selectbox(
        "停損輸入方式",
        ["不設定", "價格", "百分比"],
        index=1 if sl_default > 0 else 0,
        key=f"{key_prefix}_stop_loss_mode",
    )
    sl_default_pct = (
        max(0.0, (1 - sl_default / trade_price) * 100)
        if sl_default > 0 and trade_price > 0
        else 8.0
    )
    sl_value = 0.0
    if sl_mode != "不設定":
        sl_value = risk_cols[3].number_input(
            "停損價格 / %",
            min_value=0.0,
            value=sl_default if sl_mode == "價格" else sl_default_pct,
            step=0.5,
            key=f"{key_prefix}_stop_loss_value_{sl_mode}",
        )
    else:
        risk_cols[3].caption("未設定")

    take_profit_price = _normalize_price_target_input(
        trade_price,
        tp_value,
        tp_mode,
        is_take_profit=True,
    )
    stop_loss_price = _normalize_price_target_input(
        trade_price,
        sl_value,
        sl_mode,
        is_take_profit=False,
    )

    hint_parts = []
    if take_profit_price:
        hint_parts.append(f"停利目標 ${take_profit_price:,.2f}")
    if stop_loss_price:
        hint_parts.append(f"停損目標 ${stop_loss_price:,.2f}")
    st.caption(" ｜ ".join(hint_parts) if hint_parts else "未設定停利 / 停損目標")
    return take_profit_price, stop_loss_price


def get_trade_notes_default(action):
    return TRADE_NOTES_TEMPLATE if action == "Buy" else ""


def sync_trade_notes_template(action, notes_key, action_state_key):
    prev_action = st.session_state.get(action_state_key)
    current_notes = st.session_state.get(notes_key)

    if current_notes is None:
        st.session_state[notes_key] = get_trade_notes_default(action)
    elif prev_action and prev_action != action:
        if action == "Buy" and not str(current_notes).strip():
            st.session_state[notes_key] = TRADE_NOTES_TEMPLATE
        elif action != "Buy" and str(current_notes).strip() == TRADE_NOTES_TEMPLATE.strip():
            st.session_state[notes_key] = ""

    st.session_state[action_state_key] = action


def reset_manual_trade_form_state_if_needed():
    if not st.session_state.pop("manual_trade_reset_pending", False):
        return

    for state_key in [
        "manual_trade_notes",
        "manual_trade_last_action",
        "manual_trade_stock",
        "manual_trade_price",
        "manual_trade_shares",
        "manual_trade_disposed",
        "manual_trade_technical_score",
        "manual_trade_chip_score",
        "manual_trade_theme_score",
        "manual_trade_take_profit_mode",
        "manual_trade_take_profit_value_價格",
        "manual_trade_take_profit_value_百分比",
        "manual_trade_stop_loss_mode",
        "manual_trade_stop_loss_value_價格",
        "manual_trade_stop_loss_value_百分比",
    ]:
        if state_key in st.session_state:
            del st.session_state[state_key]


def get_trade_score_value(row, score_col, fallback=3):
    score_val = row.get(score_col, fallback)
    if pd.isna(score_val):
        score_val = fallback
    return int(score_val)


def get_trade_risk_targets_for_display(row):
    take_profit_price = row.get("take_profit_price")
    stop_loss_price = row.get("stop_loss_price")
    if pd.isna(take_profit_price) or not take_profit_price:
        fallback_tp, fallback_sl = extract_trade_risk_targets_from_notes(
            row.get("trading_notes", "")
        )
        take_profit_price = fallback_tp
        if pd.isna(stop_loss_price) or not stop_loss_price:
            stop_loss_price = fallback_sl
    elif pd.isna(stop_loss_price) or not stop_loss_price:
        _, fallback_sl = extract_trade_risk_targets_from_notes(
            row.get("trading_notes", "")
        )
        stop_loss_price = fallback_sl
    take_profit_price = pd.to_numeric(take_profit_price, errors="coerce")
    stop_loss_price = pd.to_numeric(stop_loss_price, errors="coerce")
    return (
        None if pd.isna(take_profit_price) or float(take_profit_price) <= 0 else float(take_profit_price),
        None if pd.isna(stop_loss_price) or float(stop_loss_price) <= 0 else float(stop_loss_price),
    )


def render_trade_score_summary(row, key_prefix):
    score_items = [
        ("技術面評分", get_trade_score_value(row, "technical_score"), "#38bdf8"),
        ("籌碼面評分", get_trade_score_value(row, "chip_score"), "#f97316"),
        ("題材面評分", get_trade_score_value(row, "theme_score"), "#a78bfa"),
    ]
    score_cols = st.columns(3)
    for col, (label, score_value, bar_color) in zip(score_cols, score_items):
        fill_pct = max(0, min(100, int(score_value) * 20))
        col.markdown(
            f"""
<div class="trade-score-card" id="{html.escape(str(key_prefix))}_{html.escape(label)}">
  <div class="trade-score-head">
    <span class="trade-score-label">{html.escape(label)}</span>
    <span class="trade-score-value">{int(score_value)}/5</span>
  </div>
  <div class="trade-score-track">
    <div class="trade-score-fill" style="width:{fill_pct}%;background:{bar_color};"></div>
  </div>
</div>
            """.strip(),
            unsafe_allow_html=True,
        )


def render_structured_trade_notes(notes_text):
    cleaned_notes = strip_trade_risk_metadata(notes_text)
    if not cleaned_notes:
        st.caption("無操作心得紀錄")
        return
    st.markdown("##### 結構化交易心得")
    safe_notes = html.escape(cleaned_notes).replace("\n", "<br>")
    st.markdown(
        f"<div class='trade-note-box'>{safe_notes}</div>",
        unsafe_allow_html=True,
    )


def render_trade_risk_target_summary(row):
    take_profit_price, stop_loss_price = get_trade_risk_targets_for_display(row)
    risk_cols = st.columns(2)
    risk_cols[0].metric(
        "停利目標",
        f"${take_profit_price:,.2f}" if take_profit_price else "未設定",
    )
    risk_cols[1].metric(
        "停損目標",
        f"${stop_loss_price:,.2f}" if stop_loss_price else "未設定",
    )


def render_big_value_card(
    label,
    value_text,
    value_color="#38bdf8",
    sub_text="",
    card_class="",
):
    safe_label = html.escape(str(label or ""))
    safe_value = html.escape(str(value_text or "-"))
    safe_sub = html.escape(str(sub_text or ""))
    safe_card_class = html.escape(str(card_class or ""))
    sub_html = (
        f"<div class='big-value-sub'>{safe_sub}</div>"
        if safe_sub
        else "<div class='big-value-sub'>&nbsp;</div>"
    )
    st.markdown(
        f"""
        <div class="big-value-card {safe_card_class}">
          <div class="big-value-label">{safe_label}</div>
          <div class="big-value-number" style="color:{value_color}">{safe_value}</div>
          {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dual_pnl_value_card(
    label,
    gain_pct,
    gain_amount,
    loss_pct,
    loss_amount,
):
    st.markdown(
        f"""
        <div class="big-value-card holdings-stat-card trade-cycle-dual-card">
          <div class="big-value-label">{html.escape(str(label or ""))}</div>
          <div class="trade-cycle-dual-body">
            <div class="trade-cycle-dual-line" style="color:{tw_profit_color(float(gain_amount or 0))};">
              最大浮盈 {float(gain_pct or 0):+.2f}% ｜ ${float(gain_amount or 0):+,.0f}
            </div>
            <div class="trade-cycle-dual-line" style="color:{tw_profit_color(float(loss_amount or 0))};">
              最大浮虧 {float(loss_pct or 0):+.2f}% ｜ ${float(loss_amount or 0):+,.0f}
            </div>
          </div>
          <div class="big-value-sub">&nbsp;</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_key_value_rows(rows):
    rows_html = []
    for item in rows:
        label = html.escape(str(item.get("label", "-")))
        value = html.escape(str(item.get("value", "-")))
        value_color = item.get("color", "#f8fafc")
        rows_html.append(
            "<div class='cycle-kv-row'>"
            f"<span class='cycle-kv-label'>{label}</span>"
            f"<span class='cycle-kv-value' style='color:{value_color};'>{value}</span>"
            "</div>"
        )
    st.markdown(
        f"<div class='cycle-kv-card'>{''.join(rows_html)}</div>",
        unsafe_allow_html=True,
    )


def render_cycle_flow_summary_card(selected_cycle):
    entry_date = html.escape(str(selected_cycle.get("進場日", "-")))
    exit_date = html.escape(str(selected_cycle.get("出場日", "-")))
    entry_price = f"${float(selected_cycle.get('進場均價', 0) or 0):,.2f}"
    exit_price = f"${float(selected_cycle.get('出場均價', 0) or 0):,.2f}"
    holding_days = int(selected_cycle.get("持有天數", 0) or 0)
    capital_used = f"${float(selected_cycle.get('投入資金', 0) or 0):,.0f}"
    return_pct = float(selected_cycle.get("結算報酬率 (%)", 0) or 0)
    pnl_amount = float(selected_cycle.get("結算損益", 0) or 0)
    pnl_color = tw_profit_color(pnl_amount)

    st.markdown(
        f"""
        <div class="cycle-flow-card">
          <div class="cycle-flow-endpoint">
            <div class="cycle-flow-tag cycle-flow-entry-tag">進場</div>
            <div class="cycle-flow-date">{entry_date}</div>
            <div class="cycle-flow-price">{entry_price}</div>
          </div>
          <div class="cycle-flow-center">
            <div class="cycle-flow-arrow">→</div>
            <div class="cycle-flow-days">持有 {holding_days} 天</div>
            <div class="cycle-flow-pnl" style="color:{pnl_color};">
              {return_pct:+.2f}% ｜ ${pnl_amount:+,.0f}
            </div>
          </div>
          <div class="cycle-flow-endpoint cycle-flow-exit">
            <div class="cycle-flow-tag cycle-flow-exit-tag">出場</div>
            <div class="cycle-flow-date">{exit_date}</div>
            <div class="cycle-flow-price">{exit_price}</div>
          </div>
        </div>
        <div class="cycle-flow-footnote">投入資金 {capital_used}</div>
        """,
        unsafe_allow_html=True,
    )


def build_trade_cycle_segments_from_journal(processed_df):
    if processed_df is None or processed_df.empty:
        return []

    sorted_df = processed_df.copy()
    sorted_df["date"] = pd.to_datetime(sorted_df["date"], errors="coerce")
    sorted_df = sorted_df.dropna(subset=["date"]).sort_values("date", kind="stable")
    sorted_df["date"] = sorted_df["date"].dt.strftime("%Y-%m-%d")

    cycle_segments = []
    current_rows = []
    cycle_no = 0
    running_shares = 0

    for _, row in sorted_df.iterrows():
        action = str(row.get("action", "")).strip()
        row_shares_after = int(row.get("accum_shares", 0) or 0)
        if running_shares <= 0 and action in ["Buy", "Add", "Setup"]:
            cycle_no += 1
            current_rows = []
        current_rows.append(row)
        running_shares = row_shares_after
        if action == "Close" and running_shares <= 0 and current_rows:
            current_cycle_df = pd.DataFrame(current_rows).copy()
            cycle_segments.append(
                {
                    "cycle_no": cycle_no,
                    "status": "已結案",
                    "start_date": current_rows[0]["date"],
                    "end_date": current_rows[-1]["date"],
                    "rows": current_cycle_df,
                    "cycle_realized_pnl": float(
                        pd.to_numeric(
                            current_cycle_df.get("realized_pnl", 0),
                            errors="coerce",
                        ).fillna(0).sum()
                    ),
                }
            )
            current_rows = []

    if current_rows:
        current_cycle_df = pd.DataFrame(current_rows).copy()
        cycle_segments.append(
            {
                "cycle_no": cycle_no if cycle_no > 0 else 1,
                "status": "未結案",
                "start_date": current_rows[0]["date"],
                "end_date": current_rows[-1]["date"],
                "rows": current_cycle_df,
                "cycle_realized_pnl": float(
                    pd.to_numeric(
                        current_cycle_df.get("realized_pnl", 0),
                        errors="coerce",
                    ).fillna(0).sum()
                ),
            }
        )

    return list(reversed(cycle_segments))


def render_ai_trade_cycle_review_panel(
    current_pid,
    stock_id,
    stock_display_name,
    selected_cycle_no,
    selected_cycle_summary_row,
    selected_cycle_trade_df,
):
    st.markdown("#### AI 操作評分與總和復盤")
    normalized_stock_id = normalize_stock_id(stock_id)
    review_key = f"ai_cycle_review_{current_pid}_{normalized_stock_id}_{selected_cycle_no}"
    if review_key not in st.session_state and selected_cycle_no is not None:
        st.session_state[review_key] = get_trade_cycle_ai_review(
            current_pid,
            normalized_stock_id,
            selected_cycle_no,
        )

    left_col, right_col = st.columns([1, 2])
    with left_col:
        if st.button(
            "🤖 產生 / 更新 AI 復盤",
            key=f"generate_{review_key}",
            use_container_width=True,
            disabled=selected_cycle_summary_row is None,
        ):
            with st.spinner("AI 正在閱讀這一輪所有交易資料、心得與每日路徑，整理復盤結論..."):
                review_text = ai_review_closed_trade_cycle(
                    stock_display_name,
                    selected_cycle_summary_row,
                    selected_cycle_trade_df,
                )
                st.session_state[review_key] = review_text
                save_trade_cycle_ai_review(
                    current_pid,
                    normalized_stock_id,
                    selected_cycle_no,
                    review_text,
                )

        st.caption(
            "AI 會用這一輪的逐筆交易、停利停損設定、每日未實現損益路徑與結案統計做復盤；若沒有 Gemini Key，會自動退回系統規則版摘要。"
        )

    with right_col:
        review_text = st.session_state.get(review_key)
        if review_text:
            with st.expander("查看 AI 復盤內容", expanded=False):
                st.markdown(str(review_text))
        else:
            st.info("按左側按鈕後，這裡會生成該輪次的 AI 操作評分與復盤。")


def render_dashboard_stat_card(
    label,
    value_text,
    value_color="#f8fafc",
    sub_text="",
    accent_color="#38bdf8",
):
    safe_label = html.escape(str(label or ""))
    safe_value = html.escape(str(value_text or "-"))
    safe_sub = html.escape(str(sub_text or ""))
    sub_html = (
        f"<div class='dashboard-stat-sub'>{safe_sub}</div>"
        if safe_sub
        else ""
    )
    st.markdown(
        f"""
        <div class="dashboard-stat-card" style="border-color:{accent_color}33;">
          <div class="dashboard-stat-topline" style="background:{accent_color};"></div>
          <div class="dashboard-stat-label">{safe_label}</div>
          <div class="dashboard-stat-value" style="color:{value_color};">{safe_value}</div>
          {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard_hero_panel(
    nav_value,
    day_delta,
    day_delta_pct,
    total_delta,
    total_delta_pct,
    holdings_value,
):
    day_color = tw_profit_color(float(day_delta or 0))
    total_color = tw_profit_color(float(total_delta_pct or 0))
    st.markdown(
        f"""
        <div class="dashboard-hero-panel">
          <div class="dashboard-hero-label">總資產淨值 NAV</div>
          <div class="dashboard-hero-nav">${float(nav_value or 0):,.0f}</div>
          <div class="dashboard-hero-divider"></div>
          <div class="dashboard-hero-row">
            <div class="dashboard-hero-item">
              <div class="dashboard-hero-item-label">當日總資金變化</div>
              <div class="dashboard-hero-item-value" style="color:{day_color};">
                ${float(day_delta or 0):+,.0f}
              </div>
              <div class="dashboard-hero-item-sub" style="color:{day_color};">
                {float(day_delta_pct or 0):+.2f}%
              </div>
            </div>
            <div class="dashboard-hero-item">
              <div class="dashboard-hero-item-label">累積報酬</div>
              <div class="dashboard-hero-item-value" style="color:{total_color};">
                {float(total_delta_pct or 0):+.2f}%
              </div>
              <div class="dashboard-hero-item-sub" style="color:{total_color};">
                累積損益 ${float(total_delta or 0):+,.0f}
              </div>
            </div>
            <div class="dashboard-hero-item">
              <div class="dashboard-hero-item-label">持倉總市值</div>
              <div class="dashboard-hero-item-value" style="color:#38bdf8;">
                ${float(holdings_value or 0):,.0f}
              </div>
              <div class="dashboard-hero-item-sub">含庫存估值</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard_metric_strip(metric_items):
    rows_html = []
    for item in metric_items:
        label = html.escape(str(item.get("label", "-")))
        value = html.escape(str(item.get("value", "-")))
        sub_text = html.escape(str(item.get("sub_text", "")))
        value_color = item.get("value_color", "#f8fafc")
        accent_color = item.get("accent_color", "#38bdf8")
        rows_html.append(
            "<div class='dashboard-inline-metric'>"
            "<div class='dashboard-inline-metric-head'>"
            f"<div class='dashboard-strip-dot' style='background:{accent_color};'></div>"
            f"<div class='dashboard-strip-label'>{label}</div>"
            "</div>"
            f"<div class='dashboard-strip-value' style='color:{value_color};'>{value}</div>"
            f"<div class='dashboard-strip-sub'>{sub_text}</div>"
            "</div>"
        )

    st.markdown(
        f"<div class='dashboard-inline-strip'>{''.join(rows_html)}</div>",
        unsafe_allow_html=True,
    )


def build_allocation_history_df(nav_source_df, display_name_map=None):
    if nav_source_df is None or nav_source_df.empty:
        return pd.DataFrame()

    rows = []
    for _, row in nav_source_df.iterrows():
        try:
            allocation_obj = json.loads(row.get("AllocationJson", "{}") or "{}")
        except Exception:
            allocation_obj = {}
        for asset_key, payload in allocation_obj.items():
            asset_display = asset_key
            if asset_key != "可用現金 (T+0)" and display_name_map:
                asset_display = display_name_map.get(asset_key, asset_key)
            rows.append(
                {
                    "日期": pd.to_datetime(row.get("Date"), errors="coerce"),
                    "資產": asset_display,
                    "配置金額": float((payload or {}).get("value", 0) or 0),
                    "配置比例(%)": float((payload or {}).get("weight_pct", 0) or 0),
                }
            )

    alloc_df = pd.DataFrame(rows)
    if alloc_df.empty:
        return alloc_df
    alloc_df = alloc_df.dropna(subset=["日期"]).sort_values(["日期", "配置金額"])
    alloc_df["日期"] = alloc_df["日期"].dt.strftime("%Y-%m-%d")
    return alloc_df


def render_allocation_ratio_table(allocation_df, color_map):
    rows_html = []
    for _, row in allocation_df.iterrows():
        asset_name = str(row.get("標的名稱", "-") or "-")
        swatch_color = color_map.get(asset_name, "#38bdf8")
        weight_pct = float(row.get("配置比例(%)", 0) or 0)
        market_value = float(row.get("總市值", 0) or 0)
        rows_html.append(
            "<div class='allocation-ratio-row'>"
            "<div class='allocation-asset-cell'>"
            f"<span class='allocation-color-dot' style='background:{swatch_color};'></span>"
            f"<span class='allocation-asset-name'>{html.escape(asset_name)}</span>"
            "</div>"
            f"<div class='allocation-weight-cell'>{weight_pct:.2f}%</div>"
            f"<div class='allocation-value-cell'>${market_value:,.0f}</div>"
            "</div>"
        )

    table_html = (
        "<div class='allocation-ratio-card'>"
        "<div class='allocation-ratio-header'>"
        "<span>資產項目</span>"
        "<span>配置比例</span>"
        "<span>總市值</span>"
        "</div>"
        f"{''.join(rows_html)}"
        "</div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


def render_closed_trade_cycle_summary(
    cycle_summary_df,
    key_prefix="closed_cycle",
    selected_cycle_no=None,
):
    if cycle_summary_df is None or cycle_summary_df.empty:
        st.info("這檔標的目前還沒有完整 Close 結案紀錄。")
        return None

    summary_df = cycle_summary_df.sort_values("輪次", ascending=False).reset_index(drop=True)
    selected_cycle_idx = 0
    if selected_cycle_no is not None and "輪次" in summary_df.columns:
        matched_idx = summary_df.index[
            pd.to_numeric(summary_df["輪次"], errors="coerce").fillna(-1).astype(int)
            == int(selected_cycle_no)
        ].tolist()
        if matched_idx:
            selected_cycle_idx = int(matched_idx[0])
    selected_cycle = summary_df.iloc[int(selected_cycle_idx)]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_big_value_card(
            f"第 {int(selected_cycle['輪次'])} 輪結案損益",
            f"${float(selected_cycle['結算損益']):+,.0f}",
            tw_profit_color(float(selected_cycle["結算損益"])),
            f"{selected_cycle['進場日']} → {selected_cycle['出場日']}",
            "holdings-stat-card",
        )
    with c2:
        render_big_value_card(
            "本輪結案報酬率",
            f"{float(selected_cycle['結算報酬率 (%)']):+.2f}%",
            tw_profit_color(float(selected_cycle["結算報酬率 (%)"])),
            f"持有 {int(selected_cycle['持有天數'])} 天",
            "holdings-stat-card",
        )
    with c3:
        max_profit_amt = float(selected_cycle["最大浮盈"])
        max_loss_amt = float(selected_cycle["最大浮虧"])
        max_profit_pct = float(selected_cycle["最大浮盈率 (%)"])
        max_loss_pct = float(selected_cycle["最大浮虧率 (%)"])
        render_dual_pnl_value_card(
            "最大浮盈 / 最大浮虧",
            max_profit_pct,
            max_profit_amt,
            max_loss_pct,
            max_loss_amt,
        )
    with c4:
        latest_nav_ratio = pd.to_numeric(
            selected_cycle.get("最大投入佔NAV (%)"),
            errors="coerce",
        )
        render_big_value_card(
            "最大投入資金 / 佔NAV",
            f"${float(selected_cycle['最大投入成本']):,.0f}",
            "#38bdf8",
            "-" if pd.isna(latest_nav_ratio) else f"{float(latest_nav_ratio):.2f}%",
            "holdings-stat-card",
        )

    st.markdown("##### 本輪結案完整摘要")
    detail_c1, detail_c2 = st.columns([1.35, 1])
    selected_nav_ratio = pd.to_numeric(
        selected_cycle.get("最大投入佔NAV (%)"),
        errors="coerce",
    )
    selected_weight_ratio = pd.to_numeric(
        selected_cycle.get("最大持倉比重 (%)"),
        errors="coerce",
    )
    selected_take_profit = pd.to_numeric(
        selected_cycle.get("停利目標價"),
        errors="coerce",
    )
    selected_stop_loss = pd.to_numeric(
        selected_cycle.get("停損目標價"),
        errors="coerce",
    )
    with detail_c1:
        render_cycle_flow_summary_card(selected_cycle)
    with detail_c2:
        render_key_value_rows(
            [
                {
                    "label": "最大投入佔NAV",
                    "value": "-" if pd.isna(selected_nav_ratio) else f"{float(selected_nav_ratio):.2f}%",
                },
                {
                    "label": "最大持倉比重",
                    "value": "-"
                    if pd.isna(selected_weight_ratio)
                    else f"{float(selected_weight_ratio):.2f}%",
                },
                {
                    "label": "最大浮盈",
                    "value": f"{max_profit_pct:+.2f}% / ${max_profit_amt:+,.0f}",
                    "color": tw_profit_color(max_profit_amt),
                },
                {
                    "label": "最大浮虧",
                    "value": f"{max_loss_pct:+.2f}% / ${max_loss_amt:+,.0f}",
                    "color": tw_profit_color(max_loss_amt),
                },
                {
                    "label": "停利目標價",
                    "value": "-"
                    if pd.isna(selected_take_profit)
                    else f"${float(selected_take_profit):,.2f}",
                },
                {
                    "label": "停損目標價",
                    "value": "-"
                    if pd.isna(selected_stop_loss)
                    else f"${float(selected_stop_loss):,.2f}",
                },
            ]
        )

    if len(summary_df) > 1:
        with st.expander("查看所有結案輪次總覽", expanded=False):
            overview_df = summary_df[
                [
                    "輪次",
                    "進場日",
                    "出場日",
                    "持有天數",
                    "結算損益",
                    "結算報酬率 (%)",
                    "最大浮盈率 (%)",
                    "最大浮虧率 (%)",
                    "平均操作評分",
                ]
            ].copy()
            st.dataframe(
                overview_df.style.format(
                    {
                        "結算損益": "${:+,.0f}",
                        "結算報酬率 (%)": "{:+.2f}%",
                        "最大浮盈率 (%)": "{:+.2f}%",
                        "最大浮虧率 (%)": "{:+.2f}%",
                        "平均操作評分": "{:.2f}",
                    },
                    na_rep="-",
                ).map(
                    lambda v: f"color: {tw_profit_color(float(v))}"
                    if pd.notna(pd.to_numeric(v, errors="coerce"))
                    else "color: white",
                    subset=["結算損益", "結算報酬率 (%)", "最大浮盈率 (%)", "最大浮虧率 (%)"],
                ),
                use_container_width=True,
                hide_index=True,
                height=min(320, 72 + 42 * len(overview_df)),
            )

    st.markdown("##### 本輪資金佔比與未實現損益路徑")
    try:
        path_rows = json.loads(selected_cycle.get("過程日資料", "[]") or "[]")
    except Exception:
        path_rows = []
    path_df = pd.DataFrame(path_rows)
    if path_df.empty:
        st.info("這一輪暫無可視化路徑資料。")
        return

    path_df["date"] = pd.to_datetime(path_df["date"], errors="coerce")
    path_df = path_df.dropna(subset=["date"]).sort_values("date")
    path_df["日期"] = path_df["date"].dt.strftime("%Y-%m-%d")
    for col in ["cost_ratio_pct", "allocation_weight_pct", "floating_pct", "floating_pnl"]:
        path_df[col] = pd.to_numeric(path_df[col], errors="coerce").fillna(0.0)
    path_df["close_price"] = pd.to_numeric(path_df.get("close_price"), errors="coerce").fillna(0.0)
    path_df["avg_cost"] = pd.to_numeric(path_df.get("avg_cost"), errors="coerce").fillna(0.0)
    path_df["shares"] = pd.to_numeric(path_df.get("shares"), errors="coerce").fillna(0).astype(int)

    p_left, p_right = st.columns([1.2, 1])
    with p_left:
        fig_exposure = go.Figure()
        fig_exposure.add_trace(
            go.Scatter(
                x=path_df["日期"],
                y=path_df["cost_ratio_pct"],
                mode="lines+markers",
                name="投入成本佔NAV",
                line=dict(color="#38bdf8", width=2.6),
            )
        )
        fig_exposure.add_trace(
            go.Scatter(
                x=path_df["日期"],
                y=path_df["allocation_weight_pct"],
                mode="lines+markers",
                name="市值配置比例",
                line=dict(color="#f59e0b", width=2.2, dash="dot"),
            )
        )
        fig_exposure.update_layout(
            template="plotly_dark",
            height=300,
            margin=dict(t=20, b=20, l=10, r=10),
            legend=dict(orientation="h", y=1.05, x=0),
        )
        fig_exposure.update_xaxes(type="category", title="日期", tickangle=-35)
        fig_exposure.update_yaxes(title="佔總資金比例 (%)", ticksuffix="%")
        st.plotly_chart(fig_exposure, use_container_width=True)
    with p_right:
        fig_float = go.Figure()
        fig_float.add_trace(
            go.Scatter(
                x=path_df["日期"],
                y=path_df["floating_pnl"],
                name="浮動損益",
                mode="lines+markers",
                line=dict(color="#f97316", width=3),
                fill="tozeroy",
                fillcolor="rgba(249,115,22,0.25)",
                customdata=np.stack(
                    [
                        path_df["floating_pct"],
                        path_df["close_price"],
                        path_df["avg_cost"],
                        path_df["shares"],
                    ],
                    axis=-1,
                ),
                hovertemplate=(
                    "日期 %{x}<br>"
                    "未實現損益 $%{y:,.0f}<br>"
                    "未實現報酬 %{customdata[0]:+.2f}%<br>"
                    "收盤價 $%{customdata[1]:,.2f}<br>"
                    "持股均價 $%{customdata[2]:,.2f}<br>"
                    "持有股數 %{customdata[3]:,} 股"
                    "<extra></extra>"
                ),
            )
        )
        fig_float.update_layout(
            template="plotly_dark",
            height=300,
            margin=dict(t=35, b=20, l=10, r=10),
            showlegend=False,
            title="持有期間未實現損益走勢",
        )
        fig_float.update_xaxes(type="category", title="日期", tickangle=-35)
        fig_float.update_yaxes(title="浮動損益 ($)")
        st.plotly_chart(fig_float, use_container_width=True)
        st.caption("這張圖表示：若該日收盤直接出場，這一輪部位相對當時加權均價的帳面盈虧變化。")
    return selected_cycle


def set_ai_parsed_trade_group(parsed_list):
    if not parsed_list:
        st.session_state.pop("parsed_trades_group", None)
        st.session_state["parsed_trade_idx"] = 0
        st.session_state["ai_multi_select_version"] = (
            st.session_state.get("ai_multi_select_version", 0) + 1
        )
        return

    normalized_group = []
    next_uid = int(st.session_state.get("parsed_trade_uid_counter", 0) or 0)
    for trade_item in parsed_list:
        item_copy = dict(trade_item or {})
        if "_parsed_uid" not in item_copy:
            next_uid += 1
            item_copy["_parsed_uid"] = f"ai_item_{next_uid}"
        normalized_group.append(item_copy)

    st.session_state["parsed_trade_uid_counter"] = next_uid
    st.session_state["parsed_trades_group"] = normalized_group
    st.session_state["parsed_trade_idx"] = min(
        int(st.session_state.get("parsed_trade_idx", 0) or 0),
        max(len(parsed_list) - 1, 0),
    )
    st.session_state["ai_multi_select_version"] = (
        st.session_state.get("ai_multi_select_version", 0) + 1
    )


def pop_ai_parsed_trade_at(remove_idx):
    trades = list(st.session_state.get("parsed_trades_group") or [])
    if 0 <= int(remove_idx) < len(trades):
        trades.pop(int(remove_idx))
    st.session_state["parsed_trade_idx"] = min(int(remove_idx), max(len(trades) - 1, 0))
    set_ai_parsed_trade_group(trades)
    return len(trades)


def render_trade_entry_panel(current_pid):
    tab_ai, tab_manual = st.tabs(["🤖 AI 辨識入帳", "⌨️ 手動新增"])
    default_trade_date = pd.to_datetime(get_latest_tw_trading_date()).date()

    with tab_ai:
        st.subheader("🤖 AI 交易辨識")
        uploaded_file = st.file_uploader(
            "上傳交易截圖",
            type=["png", "jpg", "jpeg"],
            key="popover_ai_uploader",
        )
        if uploaded_file is not None:
            if st.button("AI 自動辨識", key="ai_process_btn", use_container_width=True):
                with st.spinner("Gemini API 解析中..."):
                    image_bytes = uploaded_file.read()
                    parsed_list = ai_vision_single_trade(image_bytes)
                    if parsed_list:
                        st.session_state["parsed_trade_idx"] = 0
                        set_ai_parsed_trade_group(parsed_list)

                        is_mock = parsed_list[0].get("_mock", False)
                        if is_mock:
                            st.warning(
                                "⚠️ 目前使用模擬資料（請在 .env 檔案設定 GEMINI_API_KEY）"
                            )
                        else:
                            st.success(
                                f"✅ AI 辨識完成！共識別 {len(parsed_list)} 筆交易"
                            )

        if (
            "parsed_trades_group" in st.session_state
            and st.session_state["parsed_trades_group"]
        ):
            trades = st.session_state["parsed_trades_group"]
            st.divider()
            st.subheader("🔍 AI 辨識結果核對")

            if len(trades) > 1:
                labels = [
                    f"#{i + 1}: 【{t.get('stock_id')}】{t.get('price')}元"
                    for i, t in enumerate(trades)
                ]
                select_idx = min(
                    int(st.session_state.get("parsed_trade_idx", 0) or 0),
                    max(len(trades) - 1, 0),
                )
                st.session_state["parsed_trade_idx"] = select_idx
                idx = st.selectbox(
                    "請選擇要匯入的項目",
                    range(len(trades)),
                    index=select_idx,
                    format_func=lambda i: labels[i],
                    key=f"ai_multi_select_{st.session_state.get('ai_multi_select_version', 0)}",
                )
                st.session_state["parsed_trade_idx"] = idx
            else:
                idx = 0
                st.session_state["parsed_trade_idx"] = 0

            parsed = trades[idx]
            stock_key = str(parsed.get("_parsed_uid") or f"ai_{idx}_{parsed.get('stock_id')}")
            parsed_date = pd.to_datetime(
                parsed.get("trade_date") or parsed.get("date"),
                errors="coerce",
            )
            parsed_default_date = (
                parsed_date.date() if not pd.isna(parsed_date) else default_trade_date
            )
            parsed_side = str(parsed.get("side") or "").strip().lower()
            parsed_name = str(parsed.get("stock_name") or "").strip()

            with st.form(
                key=f"ai_trade_form_{stock_key}",
                clear_on_submit=False,
            ):
                with st.container(border=True):
                    heading_text = parsed.get("stock_id") or parsed_name or "未辨識標的"
                    st.markdown(f"### 📑 正在核對: {heading_text}")
                    render_trade_decision_reminder()

                    c1, c2 = st.columns(2)
                    with c1:
                        t_date = st.date_input(
                            "交易日期",
                            parsed_default_date,
                            key=f"date_{stock_key}",
                        )
                        t_stock = st.text_input(
                            "股票代號",
                            value=str(parsed.get("stock_id", "")),
                            key=f"stock_{stock_key}",
                        )
                        if parsed_name:
                            st.caption(f"AI 辨識名稱：{parsed_name}")
                        if parsed_side:
                            side_text = "買進" if parsed_side == "buy" else "賣出"
                            st.caption(f"AI 判定方向：{side_text}")

                    c3, c4 = st.columns(2)
                    with c3:
                        t_price = st.number_input(
                            "價格",
                            value=float(parsed.get("price", 0.0)),
                            key=f"price_{stock_key}",
                        )
                    with c4:
                        t_shares = st.number_input(
                            "股數",
                            value=int(parsed.get("shares", 0)),
                            step=100,
                            key=f"shares_{stock_key}",
                        )

                    derived_action = process_trade_derivation(
                        current_pid,
                        {
                            "stock_id": t_stock,
                            "stock_name": parsed_name,
                            "trade_date": t_date.strftime("%Y-%m-%d"),
                            "side": parsed_side,
                            "shares": t_shares,
                        },
                    )
                    action_key = f"action_{stock_key}"
                    action_sig_key = f"action_sig_{stock_key}"
                    action_signature = json.dumps(
                        {
                            "date": t_date.strftime("%Y-%m-%d"),
                            "stock_id": str(t_stock or "").strip(),
                            "side": parsed_side,
                            "shares": int(t_shares or 0),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if st.session_state.get(action_sig_key) != action_signature:
                        st.session_state[action_key] = derived_action
                        st.session_state[action_sig_key] = action_signature

                    t_action = st.selectbox(
                        "動作",
                        ["Buy", "Add", "Reduce", "Close"],
                        key=action_key,
                    )
                    st.caption(
                        "系統會依 AI 判定的買/賣方向，結合該交易日期之前的持倉，自動推導 Buy / Add / Reduce / Close。"
                    )
                    if not str(t_stock or "").strip() and parsed_name:
                        st.warning("目前尚未自動反查到股票代號，請確認名稱或手動補上代號。")

                    is_disposed = st.checkbox("處置股", key=f"disp_{stock_key}")
                    t_take_profit_price, t_stop_loss_price = render_trade_risk_plan_inputs(
                        f"{stock_key}_ai_risk",
                        t_price,
                        trade_action=t_action,
                    )
                    t_tech_score, t_chip_score, t_theme_score = render_trade_score_inputs(
                        f"{stock_key}_ai",
                        default_scores=(3, 3, 3),
                    )
                    notes_key = f"notes_{stock_key}"
                    sync_trade_notes_template(
                        t_action,
                        notes_key,
                        f"last_action_{stock_key}",
                    )
                    t_notes = st.text_area(
                        "交易心得",
                        key=notes_key,
                        height=320,
                        help="首筆買進會預設四段式模板；加碼、減碼、平倉則預設留白。",
                    )

                    bc1, bc2, bc3 = st.columns(3)
                    ai_save = bc1.form_submit_button(
                        "✅ 寫入", use_container_width=True
                    )
                    ai_skip = bc2.form_submit_button(
                        "⏭️ 跳過", use_container_width=True
                    )
                    ai_cancel = bc3.form_submit_button(
                        "🗑️ 取消", use_container_width=True
                    )

            if ai_save:
                try:
                    with st.spinner("寫入交易紀錄中..."):
                        execute_trade(
                            current_pid,
                            t_date.strftime("%Y-%m-%d"),
                            t_stock,
                            t_action,
                            t_price,
                            t_shares,
                            is_disposed,
                            0,
                            0,
                            0,
                            0,
                            t_notes,
                            technical_score=t_tech_score,
                            chip_score=t_chip_score,
                            theme_score=t_theme_score,
                            take_profit_price=t_take_profit_price,
                            stop_loss_price=t_stop_loss_price,
                        )
                    mark_portfolio_runtime_refresh_needed(current_pid)
                    show_ui_toast(f"{t_stock} 已寫入交易紀錄")
                    remaining_trades = pop_ai_parsed_trade_at(idx)
                    if remaining_trades == 0:
                        st.session_state["show_trade_entry_dialog"] = False
                    st.rerun()
                except ValueError as trade_error:
                    st.error(str(trade_error))
            if ai_skip:
                pop_ai_parsed_trade_at(idx)
                st.rerun()
            if ai_cancel:
                set_ai_parsed_trade_group([])
                st.session_state["show_trade_entry_dialog"] = False
                st.rerun()

    with tab_manual:
        st.subheader("⌨️ 手動新增交易")
        reset_manual_trade_form_state_if_needed()
        with st.form("manual_trade_form", clear_on_submit=False):
            render_trade_decision_reminder()

            m1, m2 = st.columns(2)
            m_date = m1.date_input(
                "交易日期",
                default_trade_date,
                key="manual_trade_date",
            )
            m_stock = m2.text_input("股票代號 (純代碼)", key="manual_trade_stock")

            m3, m4 = st.columns(2)
            m_action = m3.selectbox(
                "動作",
                ["Buy", "Add", "Reduce", "Close"],
                key="manual_trade_action",
            )
            m_price = m4.number_input("價格", value=0.0, key="manual_trade_price")

            m5, m6 = st.columns(2)
            m_shares = m5.number_input(
                "股數", value=1000, step=100, key="manual_trade_shares"
            )
            m_disposed = m6.checkbox("處置股", key="manual_trade_disposed")

            m_take_profit_price, m_stop_loss_price = render_trade_risk_plan_inputs(
                "manual_trade",
                m_price,
                trade_action=m_action,
            )

            m_tech_score, m_chip_score, m_theme_score = render_trade_score_inputs(
                "manual_trade",
                default_scores=(3, 3, 3),
            )
            sync_trade_notes_template(
                m_action,
                "manual_trade_notes",
                "manual_trade_last_action",
            )
            m_notes = st.text_area(
                "交易心得",
                key="manual_trade_notes",
                height=340,
                help="首筆買進會預設四段式模板；加碼、減碼、平倉則預設留白。",
            )

            btn_cols = st.columns([2, 1])
            manual_save = btn_cols[0].form_submit_button(
                "✅ 手動寫入紀錄", use_container_width=True
            )
            manual_close = btn_cols[1].form_submit_button(
                "關閉視窗", use_container_width=True
            )

        if manual_save:
            if m_stock:
                try:
                    with st.spinner("寫入交易紀錄中..."):
                        execute_trade(
                            current_pid,
                            m_date.strftime("%Y-%m-%d"),
                            m_stock,
                            m_action,
                            m_price,
                            m_shares,
                            m_disposed,
                            0,
                            0,
                            0,
                            0,
                            m_notes,
                            technical_score=m_tech_score,
                            chip_score=m_chip_score,
                            theme_score=m_theme_score,
                            take_profit_price=m_take_profit_price,
                            stop_loss_price=m_stop_loss_price,
                        )
                    mark_portfolio_runtime_refresh_needed(current_pid)
                    show_ui_toast(f"{m_stock} 已手動歸檔")
                    st.session_state["manual_trade_reset_pending"] = True
                    st.session_state["show_trade_entry_dialog"] = False
                    st.rerun()
                except ValueError as trade_error:
                    st.error(str(trade_error))
            else:
                st.error("請輸入股票代號")
        if manual_close:
            st.session_state["show_trade_entry_dialog"] = False
            st.rerun()


def render_trade_edit_form(trade_row, current_pid):
    trade_id = int(trade_row["id"])
    action_options = ["Buy", "Add", "Reduce", "Close", "Setup"]
    current_action = (
        str(trade_row.get("action", "Buy"))
        if pd.notna(trade_row.get("action", "Buy"))
        else "Buy"
    )
    if current_action not in action_options:
        action_options = [current_action] + action_options
    fallback_take_profit, fallback_stop_loss = extract_trade_risk_targets_from_notes(
        trade_row.get("trading_notes", "")
    )
    default_take_profit_price = trade_row.get("take_profit_price")
    if pd.isna(default_take_profit_price) or not default_take_profit_price:
        default_take_profit_price = fallback_take_profit
    default_stop_loss_price = trade_row.get("stop_loss_price")
    if pd.isna(default_stop_loss_price) or not default_stop_loss_price:
        default_stop_loss_price = fallback_stop_loss

    with st.form(
        key=f"edit_trade_form_{current_pid}_{trade_id}",
        clear_on_submit=False,
    ):
        with st.container(border=True):
            e1, e2 = st.columns(2)
            edit_date = e1.date_input(
                "交易日期",
                value=pd.to_datetime(trade_row["date"]).date(),
                key=f"edit_trade_date_{current_pid}_{trade_id}",
            )
            edit_stock = e2.text_input(
                "股票代號",
                value=str(trade_row.get("stock_id", "")),
                key=f"edit_trade_stock_{current_pid}_{trade_id}",
            )

            e3, e4 = st.columns(2)
            edit_action = e3.selectbox(
                "動作",
                action_options,
                index=action_options.index(current_action),
                key=f"edit_trade_action_{current_pid}_{trade_id}",
            )
            edit_price = e4.number_input(
                "價格",
                value=float(trade_row.get("price", 0) or 0),
                min_value=0.0,
                step=0.1,
                key=f"edit_trade_price_{current_pid}_{trade_id}",
            )

            e5, e6 = st.columns(2)
            edit_shares = e5.number_input(
                "股數",
                value=int(trade_row.get("shares", 0) or 0),
                min_value=1,
                step=100,
                key=f"edit_trade_shares_{current_pid}_{trade_id}",
            )
            edit_disposed = e6.checkbox(
                "處置股",
                value=bool(trade_row.get("is_disposed", False)),
                key=f"edit_trade_disposed_{current_pid}_{trade_id}",
            )

            edit_take_profit_price, edit_stop_loss_price = render_trade_risk_plan_inputs(
                f"edit_trade_{current_pid}_{trade_id}",
                edit_price,
                default_take_profit_price=default_take_profit_price,
                default_stop_loss_price=default_stop_loss_price,
                trade_action=edit_action,
            )

            score_cols = st.columns(3)
            edit_technical_score = score_cols[0].slider(
                "技術面評分",
                1,
                5,
                value=get_trade_score_value(trade_row, "technical_score"),
                key=f"edit_trade_tech_{current_pid}_{trade_id}",
            )
            edit_chip_score = score_cols[1].slider(
                "籌碼面評分",
                1,
                5,
                value=get_trade_score_value(trade_row, "chip_score"),
                key=f"edit_trade_chip_{current_pid}_{trade_id}",
            )
            edit_theme_score = score_cols[2].slider(
                "題材面評分",
                1,
                5,
                value=get_trade_score_value(trade_row, "theme_score"),
                key=f"edit_trade_theme_{current_pid}_{trade_id}",
            )

            edit_notes = st.text_area(
                "交易心得 / 備註",
                value=strip_trade_risk_metadata(trade_row.get("trading_notes", "")),
                height=220,
                key=f"edit_trade_notes_{current_pid}_{trade_id}",
            )

            save_edit = st.form_submit_button(
                "儲存修改",
                use_container_width=True,
            )

    if save_edit:
        try:
            update_trade_record(
                current_pid,
                trade_id,
                edit_date.strftime("%Y-%m-%d"),
                edit_stock,
                edit_action,
                edit_price,
                edit_shares,
                edit_disposed,
                edit_notes,
                technical_score=edit_technical_score,
                chip_score=edit_chip_score,
                theme_score=edit_theme_score,
                take_profit_price=edit_take_profit_price,
                stop_loss_price=edit_stop_loss_price,
            )
            invalidate_portfolio_runtime_bundle(current_pid)
            show_ui_toast("交易紀錄已更新")
            st.session_state["editing_trade_id"] = None
            st.rerun()
        except ValueError as edit_error:
            st.error(str(edit_error))

    st.divider()
    confirm_delete = st.checkbox(
        "我確認要刪除這筆交易，並讓後續 NAV / 持倉 / 績效自動重算",
        key=f"delete_trade_confirm_{current_pid}_{trade_id}",
    )
    action_cols = st.columns([1, 1])
    if action_cols[0].button(
        "🗑️ 刪除這筆交易",
        use_container_width=True,
        disabled=not confirm_delete,
        key=f"delete_trade_btn_{current_pid}_{trade_id}",
    ):
        try:
            delete_trade_record(current_pid, trade_id)
            invalidate_portfolio_runtime_bundle(current_pid)
            show_ui_toast("交易紀錄已刪除")
            st.session_state["editing_trade_id"] = None
            st.rerun()
        except ValueError as delete_error:
            st.error(str(delete_error))
    if action_cols[1].button(
        "關閉編輯視窗",
        use_container_width=True,
        key=f"close_trade_edit_{current_pid}_{trade_id}",
    ):
        st.session_state["editing_trade_id"] = None
        st.rerun()


if hasattr(st, "dialog"):

    @st.dialog("✏️ 編輯交易紀錄", width="large")
    def render_trade_edit_dialog(trade_row, current_pid):
        render_trade_edit_form(trade_row, current_pid)

else:

    def render_trade_edit_dialog(trade_row, current_pid):
        st.info("目前 Streamlit 版本不支援大型彈窗，已改用頁內表單顯示。")
        render_trade_edit_form(trade_row, current_pid)


if hasattr(st, "dialog"):

    @st.dialog("➕ 新增交易 / AI 辨識", width="large")
    def render_trade_entry_dialog(current_pid):
        render_trade_entry_panel(current_pid)

else:

    def render_trade_entry_dialog(current_pid):
        st.info("目前 Streamlit 版本不支援大型彈窗，已改用頁內表單顯示。")
        render_trade_entry_panel(current_pid)


def format_notebook_update_log(update_log_json, created_date=""):
    try:
        update_dates = json.loads(update_log_json or "[]")
    except Exception:
        update_dates = []
    if not isinstance(update_dates, list):
        update_dates = []

    unique_dates = []
    for date_item in [created_date] + update_dates:
        date_text = str(date_item or "").strip()
        if date_text and date_text not in unique_dates:
            unique_dates.append(date_text)
    return "、".join(unique_dates) if unique_dates else "-"


def render_notion_editor(content_key, initial_value="", height=520):
    if content_key not in st.session_state:
        st.session_state[content_key] = str(initial_value or "")

    edit_tab, preview_tab = st.tabs(["✍️ 編輯", "👀 預覽"])
    with edit_tab:
        edited_text = st.text_area(
            "筆記內容",
            key=content_key,
            height=height,
            label_visibility="collapsed",
        )
    with preview_tab:
        preview_text = str(st.session_state.get(content_key, "") or "").strip()
        if preview_text:
            st.markdown(preview_text)
        else:
            st.caption("目前沒有內容可預覽。")
    return edited_text


def render_daily_macro_journal_tab():
    with st.expander("✍️ 撰寫今日筆記", expanded=True):
        with st.form("macro_journal_form"):
            j_date = st.date_input("寫作日期", datetime.now(), key="macro_new_date")
            q1 = st.text_area(
                "1. 大盤與整體動態",
                height=140,
                placeholder="描述今日大盤氣氛、資金流向與總經數據影響...",
                key="macro_new_q1",
            )
            q2 = st.text_area(
                "2. 交易檢討與改進",
                height=140,
                placeholder="分享今日心魔、做對與做錯的操作、後續策略調整...",
                key="macro_new_q2",
            )
            q3 = st.text_area(
                "3. 最近觀察標的與原因",
                height=140,
                placeholder="列出值得留意的個股、型態、籌碼或是突破價位...",
                key="macro_new_q3",
            )

            if st.form_submit_button("儲存入記事本"):
                combined_content = (
                    f"### 🌍 大盤與整體動態\n{q1}\n\n"
                    f"### 💡 交易檢討與改進\n{q2}\n\n"
                    f"### 🔭 最近觀察標的與原因\n{q3}"
                )
                save_macro_journal(j_date.strftime("%Y-%m-%d"), combined_content)
                show_ui_toast("筆記已發布")
                st.rerun()

    st.markdown("---")
    st.subheader("📖 歷史筆記流")

    md_df = get_macro_journals()
    if md_df.empty:
        st.info("目前筆記本還是空白的。")
        return

    for _, row in md_df.iterrows():
        note_date = str(row["date"])
        with st.expander(f"📅 {note_date}", expanded=False):
            edit_key = f"macro_note_edit_{note_date}"
            current_text = render_notion_editor(
                edit_key,
                row.get("content", ""),
                height=440,
            )
            action_cols = st.columns([1, 1, 4])
            if action_cols[0].button("儲存修改", key=f"save_macro_{note_date}"):
                save_macro_journal(note_date, current_text)
                show_ui_toast(f"{note_date} 筆記已更新")
                st.rerun()
            if action_cols[1].button("刪除筆記", key=f"delete_macro_{note_date}"):
                delete_macro_journal(note_date)
                st.session_state.pop(edit_key, None)
                show_ui_toast(f"{note_date} 筆記已刪除")
                st.rerun()


def render_article_notebook_tab(note_type, section_title, new_button_label, key_prefix):
    st.subheader(section_title)

    articles_df = get_notebook_articles(note_type).copy()
    if st.button(new_button_label, key=f"{key_prefix}_create_btn"):
        new_article_id = create_notebook_article(
            note_type,
            "未命名筆記",
            "先在這裡開始寫內容。",
            datetime.now().strftime("%Y-%m-%d"),
        )
        if new_article_id:
            st.session_state[f"{key_prefix}_selected_article_id"] = int(new_article_id)
        show_ui_toast("已建立新筆記")
        st.rerun()

    if articles_df.empty:
        st.info("目前還沒有筆記，先按上方按鈕新增一篇。")
        return

    articles_df["id"] = pd.to_numeric(articles_df["id"], errors="coerce").fillna(0).astype(int)
    article_options = [article_id for article_id in articles_df["id"].tolist() if article_id > 0]
    selected_state_key = f"{key_prefix}_selected_article_id"
    if not article_options:
        st.info("目前還沒有可讀取的筆記。")
        return
    if st.session_state.get(selected_state_key) not in article_options:
        st.session_state[selected_state_key] = article_options[0]

    selected_article_id = st.selectbox(
        "選擇筆記事項",
        options=article_options,
        format_func=lambda article_id: str(
            articles_df.loc[articles_df["id"] == article_id, "title"].iloc[0]
        ),
        key=selected_state_key,
    )
    selected_row = articles_df[articles_df["id"] == selected_article_id].iloc[0]

    meta_left, meta_right = st.columns([1.35, 1])
    with meta_left:
        title_value = st.text_input(
            "筆記標題",
            value=str(selected_row.get("title", "") or ""),
            key=f"{key_prefix}_title_{selected_article_id}",
            label_visibility="collapsed",
        )
    with meta_right:
        raw_edit_date = pd.to_datetime(
            selected_row.get("updated_at") or selected_row.get("created_date") or datetime.now(),
            errors="coerce",
        )
        edit_date = st.date_input(
            "本次編輯日期",
            value=raw_edit_date.date() if pd.notna(raw_edit_date) else datetime.now().date(),
            key=f"{key_prefix}_edit_date_{selected_article_id}",
        )

    content_key = f"{key_prefix}_content_{selected_article_id}"
    article_content = render_notion_editor(
        content_key,
        selected_row.get("content", ""),
        height=560,
    )

    update_log_text = format_notebook_update_log(
        selected_row.get("update_log_json", "[]"),
        selected_row.get("created_date", ""),
    )
    st.caption(
        f"建立日期：{selected_row.get('created_date', '-')}"
        f"｜更新日期紀錄：{update_log_text}"
    )

    save_col, delete_col, _ = st.columns([1, 1, 4])
    if save_col.button("儲存修改", key=f"{key_prefix}_save_{selected_article_id}"):
        try:
            update_notebook_article(
                selected_article_id,
                title_value,
                article_content,
                edit_date.strftime("%Y-%m-%d"),
                selected_row.get("update_log_json", "[]"),
            )
            show_ui_toast("筆記已更新")
            st.rerun()
        except ValueError as note_error:
            st.error(str(note_error))

    if delete_col.button("刪除此筆記", key=f"{key_prefix}_delete_{selected_article_id}"):
        delete_notebook_article(selected_article_id)
        st.session_state.pop(content_key, None)
        st.session_state.pop(f"{key_prefix}_title_{selected_article_id}", None)
        show_ui_toast("筆記已刪除")
        st.rerun()


from backend import (
    ai_vision_portfolio,
    ai_vision_single_trade,
    ai_review_closed_trade_cycle,
    auto_process_settlement,
    build_pending_settlement_schedule,
    calculate_trade_journal,
    calculate_twr_and_nav,
    create_notebook_article,
    create_portfolio,
    delete_macro_journal,
    delete_notebook_article,
    delete_portfolio_and_related_data,
    ensure_db_schema,
    ensure_portfolios_official_nav_synced,
    execute_cashflow,
    execute_trade,
    get_market_holidays_df,
    get_holdings_detail,
    get_latest_official_tw_trading_date,
    get_latest_tw_trading_date,
    get_macro_journals,
    get_notebook_articles,
    get_db_connection,
    get_portfolio_net_invested_amount,
    get_portfolio_state,
    get_portfolios,
    get_supabase_client,
    get_stock_display_names,
    get_trade_cycle_ai_review,
    get_full_yf_symbol,
    set_manual_price_override,
    delete_manual_price_override,
    delete_trade_record,
    normalize_stock_id,
    get_portfolio_trades_df,
    process_trade_derivation,
    save_macro_journal,
    sync_twse_market_holidays,
    summarize_closed_stock_trade_cycles,
    update_trade_record,
    update_holding_risk_targets,
    update_notebook_article,
    upsert_market_holiday,
    delete_market_holiday,
    save_trade_cycle_ai_review,
)


def show_ui_toast(message, icon="✅"):
    if hasattr(st, "toast"):
        st.toast(message, icon=icon)


def invalidate_portfolio_runtime_bundle(portfolio_id=None):
    cache_map = st.session_state.get("portfolio_runtime_bundle_cache")
    if isinstance(cache_map, dict):
        if portfolio_id is None:
            cache_map.clear()
        else:
            cache_map.pop(int(portfolio_id), None)
    clear_trade_review_runtime_cache(portfolio_id)
    st.session_state["portfolio_runtime_nonce"] = (
        int(st.session_state.get("portfolio_runtime_nonce", 0) or 0) + 1
    )


def clear_trade_review_runtime_cache(portfolio_id=None):
    review_cache_map = st.session_state.get("trade_review_runtime_cache")
    if isinstance(review_cache_map, dict):
        if portfolio_id is None:
            review_cache_map.clear()
        else:
            pid_prefix = f"{int(portfolio_id)}|"
            remove_keys = [
                cache_key
                for cache_key in review_cache_map.keys()
                if str(cache_key).startswith(pid_prefix)
            ]
            for cache_key in remove_keys:
                review_cache_map.pop(cache_key, None)


def mark_portfolio_runtime_refresh_needed(portfolio_id, reason="交易資料已更新"):
    refresh_map = st.session_state.setdefault("portfolio_runtime_refresh_needed", {})
    refresh_map[int(portfolio_id)] = reason
    clear_trade_review_runtime_cache(portfolio_id)


def render_portfolio_refresh_notice(portfolio_id):
    refresh_map = st.session_state.get("portfolio_runtime_refresh_needed", {})
    reason = refresh_map.get(int(portfolio_id)) if isinstance(refresh_map, dict) else None
    if not reason:
        return

    notice_col, button_col = st.columns([4, 1])
    notice_col.info(
        f"{reason}。交易已存入資料庫；為了讓新增/編輯流程更順，儀表板與 NAV 重算先暫停，點右側按鈕再更新。"
    )
    if button_col.button("立即更新資料", use_container_width=True, key=f"refresh_runtime_{portfolio_id}"):
        refresh_map.pop(int(portfolio_id), None)
        invalidate_portfolio_runtime_bundle(portfolio_id)
        st.rerun()


def get_portfolio_runtime_bundle(portfolio_id, official_nav_date):
    cache_map = st.session_state.setdefault("portfolio_runtime_bundle_cache", {})
    runtime_nonce = int(st.session_state.get("portfolio_runtime_nonce", 0) or 0)
    today_signature = datetime.now().strftime("%Y-%m-%d")
    cache_signature = (
        f"{int(portfolio_id)}|{official_nav_date}|{today_signature}|{runtime_nonce}"
    )
    cached_bundle = cache_map.get(int(portfolio_id))
    if cached_bundle and cached_bundle.get("signature") == cache_signature:
        return cached_bundle["data"]

    with st.spinner("載入報價與 TWR 計算中..."):
        auto_process_settlement(portfolio_id)
        t0_cash, t2_cash = get_portfolio_state(portfolio_id)
        hist_df, _, current_twr = calculate_twr_and_nav(portfolio_id)
        holdings_df, _ = get_holdings_detail(portfolio_id)

    bundle = {
        "t0_cash": t0_cash,
        "t2_cash": t2_cash,
        "hist_df": hist_df,
        "current_twr": current_twr,
        "holdings_df": holdings_df,
    }
    cache_map[int(portfolio_id)] = {"signature": cache_signature, "data": bundle}
    return bundle


@st.cache_data(ttl=300)
def fetch_trade_review_price_history(stock_id, start_date_str, end_date_str):
    yf_code = get_full_yf_symbol(stock_id)
    return yf.download(
        yf_code,
        actions=True,
        auto_adjust=True,
        start=start_date_str,
        end=end_date_str,
        progress=False,
    )


def get_trade_review_runtime_bundle(portfolio_id, stock_id, hist_df):
    cache_map = st.session_state.setdefault("trade_review_runtime_cache", {})
    runtime_nonce = int(st.session_state.get("portfolio_runtime_nonce", 0) or 0)
    hist_signature = "empty"
    if hist_df is not None and not hist_df.empty:
        hist_signature = (
            f"{len(hist_df)}|{hist_df.iloc[-1].get('Date', '')}|"
            f"{float(pd.to_numeric(hist_df.iloc[-1].get('NAV', 0), errors='coerce') or 0):.4f}"
        )
    cache_key = f"{int(portfolio_id)}|{normalize_stock_id(stock_id)}"
    cache_signature = f"{runtime_nonce}|{hist_signature}"
    cached_bundle = cache_map.get(cache_key)
    if cached_bundle and cached_bundle.get("signature") == cache_signature:
        return cached_bundle["data"]

    trades_df = get_portfolio_trades_df(portfolio_id)
    if trades_df.empty:
        bundle = {
            "trades_df": trades_df,
            "display_names": {},
            "filtered_df": pd.DataFrame(),
            "processed_df": pd.DataFrame(),
            "trade_cycle_segments": [],
            "closed_cycle_df": pd.DataFrame(),
        }
    else:
        trades_df = trades_df.copy()
        trades_df["stock_id"] = trades_df["stock_id"].apply(normalize_stock_id)
        display_names = get_stock_display_names(
            trades_df["stock_id"].dropna().unique().tolist()
        )
        normalized_stock_id = normalize_stock_id(stock_id)
        filtered_df = (
            trades_df[trades_df["stock_id"] == normalized_stock_id].copy()
            if normalized_stock_id
            else pd.DataFrame()
        )
        processed_df = (
            calculate_trade_journal(filtered_df) if not filtered_df.empty else pd.DataFrame()
        )
        trade_cycle_segments = build_trade_cycle_segments_from_journal(processed_df)
        closed_cycle_df = (
            summarize_closed_stock_trade_cycles(
                portfolio_id,
                normalized_stock_id,
                portfolio_hist_df=hist_df,
            )
            if normalized_stock_id
            else pd.DataFrame()
        )
        bundle = {
            "trades_df": trades_df,
            "display_names": display_names,
            "filtered_df": filtered_df,
            "processed_df": processed_df,
            "trade_cycle_segments": trade_cycle_segments,
            "closed_cycle_df": closed_cycle_df,
        }

    cache_map[cache_key] = {"signature": cache_signature, "data": bundle}
    return bundle


def build_closed_trade_analytics_from_trades(trades_df, display_names=None):
    if trades_df is None or trades_df.empty:
        return pd.DataFrame()

    display_names = display_names or {}
    work_df = trades_df.copy()
    work_df["stock_id"] = work_df["stock_id"].apply(normalize_stock_id)
    work_df["date"] = pd.to_datetime(work_df["date"], errors="coerce")
    work_df["id"] = pd.to_numeric(work_df.get("id"), errors="coerce").fillna(0)
    work_df = work_df.dropna(subset=["date", "stock_id"]).sort_values(
        ["stock_id", "date", "id"],
        kind="stable",
    )
    if work_df.empty:
        return pd.DataFrame()

    rows = []
    for stock_id, stock_df in work_df.groupby("stock_id", sort=False):
        journal_df = calculate_trade_journal(stock_df.copy())
        if journal_df.empty:
            continue

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
            entry_rows = cycle_df[cycle_df["action"].isin(["Buy", "Add", "Setup"])].copy()
            exit_rows = cycle_df[cycle_df["action"].isin(["Reduce", "Close"])].copy()
            entry_amount = (
                float((entry_rows["price"] * entry_rows["shares"]).sum())
                if not entry_rows.empty
                else 0.0
            )
            exit_amount = (
                float((exit_rows["price"] * exit_rows["shares"]).sum())
                if not exit_rows.empty
                else 0.0
            )
            total_dividends = float(
                cycle_df.loc[cycle_df["action"] == "Dividend", "realized_pnl"].sum()
            )
            first_cum_before = float(cycle_df.iloc[0].get("cum_pnl", 0) or 0) - float(
                cycle_df.iloc[0].get("realized_pnl", 0) or 0
            )
            total_pnl = float(cycle_df.iloc[-1].get("cum_pnl", 0) or 0) - first_cum_before
            return_pct = total_pnl / entry_amount * 100 if entry_amount > 0 else 0.0
            cycle_start = pd.to_datetime(cycle_df.iloc[0]["date"], errors="coerce")
            cycle_end = pd.to_datetime(cycle_df.iloc[-1]["date"], errors="coerce")
            holding_days = (
                max(1, int((cycle_end - cycle_start).days) + 1)
                if pd.notna(cycle_start) and pd.notna(cycle_end)
                else 1
            )

            score_means = {}
            for score_col in ["technical_score", "chip_score", "theme_score"]:
                if score_col in cycle_df.columns:
                    score_series = pd.to_numeric(cycle_df[score_col], errors="coerce").dropna()
                else:
                    score_series = pd.Series(dtype=float)
                score_means[score_col] = (
                    float(score_series.mean()) if not score_series.empty else np.nan
                )
            all_score_values = [
                score
                for score in score_means.values()
                if pd.notna(pd.to_numeric(score, errors="coerce"))
            ]

            rows.append(
                {
                    "stock_id": stock_id,
                    "標的": display_names.get(stock_id, stock_id),
                    "輪次": cycle_no,
                    "進場日": cycle_start.strftime("%Y-%m-%d") if pd.notna(cycle_start) else "-",
                    "出場日": cycle_end.strftime("%Y-%m-%d") if pd.notna(cycle_end) else "-",
                    "持有天數": holding_days,
                    "投入資金": entry_amount,
                    "出場回收": exit_amount + total_dividends,
                    "結算損益": total_pnl,
                    "報酬率 (%)": return_pct,
                    "平均技術評分": score_means["technical_score"],
                    "平均籌碼評分": score_means["chip_score"],
                    "平均產業評分": score_means["theme_score"],
                    "平均總評分": float(np.mean(all_score_values)) if all_score_values else np.nan,
                }
            )
            cycle_rows = []

    if not rows:
        return pd.DataFrame()
    result_df = pd.DataFrame(rows)
    return result_df.sort_values(["出場日", "標的", "輪次"], ascending=[False, True, False])


def render_closed_trade_analytics_panel(trades_df, display_names):
    st.markdown("### 已完成交易分析")
    st.caption("資料直接來自 Supabase 交易流水帳，僅統計已 Close 的完整交易輪次。")

    analytics_df = build_closed_trade_analytics_from_trades(trades_df, display_names)
    if analytics_df.empty:
        st.info("目前還沒有已完成且已 Close 的交易輪次，等有結案交易後這裡會自動產生統計。")
        return

    pnl_series = pd.to_numeric(analytics_df["結算損益"], errors="coerce").fillna(0.0)
    return_series = pd.to_numeric(analytics_df["報酬率 (%)"], errors="coerce").fillna(0.0)
    winners = pnl_series[pnl_series > 0]
    losers = pnl_series[pnl_series < 0]
    total_count = len(pnl_series)
    win_rate = len(winners) / total_count * 100 if total_count else 0.0
    avg_win = float(winners.mean()) if not winners.empty else 0.0
    avg_loss = float(losers.mean()) if not losers.empty else 0.0
    profit_loss_ratio = avg_win / abs(avg_loss) if avg_win > 0 and avg_loss < 0 else np.nan
    if pd.notna(profit_loss_ratio):
        profit_loss_text = f"{profit_loss_ratio:.2f}x"
    elif avg_win > 0:
        profit_loss_text = "尚無虧損"
    elif avg_loss < 0:
        profit_loss_text = "尚無獲利"
    else:
        profit_loss_text = "損益持平"
    expectancy = float(pnl_series.mean()) if total_count else 0.0

    metric_cols = st.columns(4)
    with metric_cols[0]:
        render_big_value_card(
            "勝率",
            f"{win_rate:.1f}%",
            "#ef4444" if win_rate >= 50 else "#22c55e",
            f"{len(winners)} 勝 / {total_count} 筆",
            card_class="holdings-stat-card",
        )
    with metric_cols[1]:
        render_big_value_card(
            "盈虧比",
            profit_loss_text,
            "#38bdf8",
            f"均賺 ${avg_win:,.0f}｜均虧 ${avg_loss:,.0f}",
            card_class="holdings-stat-card",
        )
    with metric_cols[2]:
        render_big_value_card(
            "單筆期望值",
            f"${expectancy:+,.0f}",
            tw_profit_color(expectancy),
            "每完成一輪的平均損益",
            card_class="holdings-stat-card",
        )
    with metric_cols[3]:
        render_big_value_card(
            "平均報酬率",
            f"{float(return_series.mean()):+.2f}%",
            tw_profit_color(float(return_series.mean())),
            f"共 {total_count} 筆已結案交易",
            card_class="holdings-stat-card",
        )

    plot_df = analytics_df.copy()
    plot_df["損益方向"] = np.where(plot_df["結算損益"] >= 0, "獲利", "虧損")
    plot_df["投入資金"] = pd.to_numeric(plot_df["投入資金"], errors="coerce").fillna(0.0)
    plot_df["持有天數"] = pd.to_numeric(plot_df["持有天數"], errors="coerce").fillna(0).astype(int)
    plot_df["報酬率 (%)"] = pd.to_numeric(plot_df["報酬率 (%)"], errors="coerce").fillna(0.0)

    responsive_plot_config = {"responsive": True, "displayModeBar": False}
    chart_left, chart_right = st.container(), st.container()
    with chart_left:
        fig_holding = px.scatter(
            plot_df,
            x="持有天數",
            y="報酬率 (%)",
            color="損益方向",
            size="投入資金",
            size_max=28,
            color_discrete_map={"獲利": "#ef4444", "虧損": "#22c55e"},
            hover_data={
                "標的": True,
                "輪次": True,
                "進場日": True,
                "出場日": True,
                "結算損益": ":+,.0f",
                "投入資金": ":,.0f",
                "損益方向": False,
            },
            title="持有天數與報酬率的關係圖",
            template="plotly_dark",
        )
        fig_holding.update_layout(
            autosize=True,
            height=430,
            margin=dict(t=52, b=48, l=44, r=18),
            legend=dict(orientation="h", y=1.02, x=0),
            font=dict(size=12),
        )
        fig_holding.update_xaxes(title="持有天數", dtick=1, rangemode="tozero")
        fig_holding.update_yaxes(title="結案報酬率", ticksuffix="%", zeroline=True)
        st.plotly_chart(fig_holding, use_container_width=True, config=responsive_plot_config)

    with chart_right:
        score_long_df = plot_df.melt(
            id_vars=["標的", "輪次", "進場日", "出場日", "結算損益", "報酬率 (%)"],
            value_vars=["平均產業評分", "平均籌碼評分", "平均技術評分"],
            var_name="評分面向",
            value_name="平均評分",
        ).dropna(subset=["平均評分"])
        score_long_df["評分面向"] = score_long_df["評分面向"].replace(
            {
                "平均產業評分": "產業/題材",
                "平均籌碼評分": "籌碼",
                "平均技術評分": "技術",
            }
        )
        score_long_df["平均評分"] = (
            pd.to_numeric(score_long_df["平均評分"], errors="coerce")
            .clip(lower=1, upper=5)
        )
        score_long_df = score_long_df.dropna(subset=["平均評分"])
        if score_long_df.empty:
            st.warning("這個資金池有已結案交易，但交易紀錄裡沒有可用的三大評分，因此暫時無法畫評分散佈圖。")
            return

        score_long_df["損益方向"] = np.where(score_long_df["結算損益"] >= 0, "獲利", "虧損")
        score_long_df["同分序號"] = score_long_df.groupby(
            ["評分面向", "平均評分"],
            sort=False,
        ).cumcount()
        score_long_df["同分總數"] = score_long_df.groupby(
            ["評分面向", "平均評分"],
            sort=False,
        )["平均評分"].transform("count")
        score_long_df["評分顯示位置"] = score_long_df["平均評分"] + (
            score_long_df["同分序號"] - (score_long_df["同分總數"] - 1) / 2
        ).clip(-4, 4) * 0.045
        st.caption(
            f"圖表資料：已結案 {len(plot_df)} 輪，三大評分散點 {len(score_long_df)} 個。"
            "若很多點集中在 3 分附近，代表歷史交易大多使用中性預設評分。"
        )
        fig_score = px.scatter(
            score_long_df,
            x="評分顯示位置",
            y="結算損益",
            color="損益方向",
            facet_row="評分面向",
            color_discrete_map={
                "獲利": "#ef4444",
                "虧損": "#22c55e",
            },
            category_orders={"評分面向": ["技術", "籌碼", "產業/題材"]},
            hover_data={
                "標的": True,
                "輪次": True,
                "進場日": True,
                "出場日": True,
                "平均評分": ":.2f",
                "評分顯示位置": False,
                "報酬率 (%)": ":+.2f",
                "結算損益": ":+,.0f",
                "損益方向": False,
            },
            labels={"評分顯示位置": "平均信心評分", "結算損益": "最終結算損益"},
            title="三大信心評分與最終損益的散佈圖",
            template="plotly_dark",
        )
        fig_score.update_traces(marker=dict(size=15, opacity=0.78, line=dict(width=1, color="#0f172a")))
        fig_score.add_hline(y=0, line_dash="dash", line_color="#64748b", line_width=1)
        fig_score.for_each_annotation(
            lambda annotation: annotation.update(text=annotation.text.split("=")[-1])
        )
        fig_score.update_layout(
            autosize=True,
            height=620,
            margin=dict(t=58, b=48, l=58, r=18),
            legend=dict(orientation="h", y=1.02, x=0),
            font=dict(size=12),
        )
        fig_score.update_xaxes(title="平均信心評分", range=[0.7, 5.3], dtick=1)
        fig_score.update_yaxes(title="最終結算損益", tickprefix="$", zeroline=True)
        st.plotly_chart(fig_score, use_container_width=True, config=responsive_plot_config)
        st.caption(
            "閱讀方式：每一列是一個評分面向；點越靠右代表該面向信心越高，越往上代表該輪交易最終損益越好。"
        )

    with st.expander("查看已完成交易明細", expanded=False):
        table_df = analytics_df[
            [
                "標的",
                "輪次",
                "進場日",
                "出場日",
                "持有天數",
                "投入資金",
                "結算損益",
                "報酬率 (%)",
                "平均產業評分",
                "平均籌碼評分",
                "平均技術評分",
            ]
        ].copy()
        st.dataframe(
            table_df.style.format(
                {
                    "投入資金": "${:,.0f}",
                    "結算損益": "${:+,.0f}",
                    "報酬率 (%)": "{:+.2f}%",
                    "平均產業評分": "{:.2f}",
                    "平均籌碼評分": "{:.2f}",
                    "平均技術評分": "{:.2f}",
                },
                na_rep="-",
            ).map(
                lambda v: f"color: {tw_profit_color(float(v))}"
                if pd.notna(pd.to_numeric(v, errors="coerce"))
                else "color: white",
                subset=["結算損益", "報酬率 (%)"],
            ),
            use_container_width=True,
            hide_index=True,
            height=min(420, 76 + 40 * len(table_df)),
        )


# --- UI Configuration & Styling ---
st.set_page_config(page_title="波段交易與 AI 記帳", layout="wide", page_icon="📈")

st.markdown(
    """
<style>
    .stApp { background-color: #0e1117; font-family: 'Inter', sans-serif; }
    .metric-card { background: transparent; padding: 8px 0; border-radius: 0; box-shadow: none; margin-bottom: 14px; color: white; }
    .metric-value { font-size: 28px; font-weight: 700; color: #38bdf8; }
    div[data-testid="stPopoverBody"] { width: min(1080px, 92vw); }
    .trade-reminder-box {
        padding: 18px 20px;
        border-radius: 18px;
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(17, 24, 39, 0.9));
        border: 1px solid rgba(56, 189, 248, 0.28);
        margin-bottom: 16px;
    }
    .reminder-title {
        font-size: 1rem;
        font-weight: 800;
        color: #7dd3fc;
        margin-bottom: 10px;
        letter-spacing: 0.08em;
    }
    .reminder-row {
        color: #e5e7eb;
        font-size: 0.95rem;
        line-height: 1.7;
        margin-bottom: 8px;
    }
    .trade-note-box {
        white-space: pre-wrap;
        padding: 16px 18px;
        border-radius: 16px;
        background: rgba(15, 23, 42, 0.7);
        border: 1px solid rgba(148, 163, 184, 0.28);
        color: #e5e7eb;
        line-height: 1.75;
    }
    .trade-score-card {
        padding: 14px 16px;
        border-radius: 18px;
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.96), rgba(15, 23, 42, 0.78));
        border: 1px solid rgba(148, 163, 184, 0.2);
        margin-bottom: 10px;
    }
    .trade-score-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 10px;
    }
    .trade-score-label {
        color: #cbd5e1;
        font-size: 13px;
        font-weight: 800;
        letter-spacing: 0.06em;
    }
    .trade-score-value {
        color: #f8fafc;
        font-size: 18px;
        font-weight: 900;
        font-variant-numeric: tabular-nums;
    }
    .trade-score-track {
        width: 100%;
        height: 12px;
        border-radius: 999px;
        background: rgba(30, 41, 59, 0.9);
        overflow: hidden;
    }
    .trade-score-fill {
        height: 100%;
        border-radius: 999px;
    }
    .big-value-card {
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        box-sizing: border-box;
        border: 0;
        border-radius: 0;
        padding: 8px 2px;
        background: transparent;
        box-shadow: none;
        min-height: 128px;
        height: auto;
    }
    .big-value-label {
        color: #94a3b8;
        font-size: 13px;
        font-weight: 800;
        letter-spacing: 0.08em;
        line-height: 1.35;
        min-height: 34px;
    }
    .big-value-number {
        max-width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        font-size: clamp(1.45rem, 2vw, 1.9rem);
        font-weight: 800;
        line-height: 1.15;
        white-space: nowrap;
        letter-spacing: -0.03em;
        margin-top: auto;
        font-variant-numeric: tabular-nums;
    }
    .big-value-sub {
        min-height: 22px;
        margin-top: 8px;
        font-size: 13px;
        line-height: 1.35;
        color: #cbd5e1;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .holdings-stat-card {
        min-height: 128px;
        height: auto;
    }
    .trade-cycle-dual-card {
        min-height: 168px;
        height: 168px;
        justify-content: flex-start;
    }
    .trade-cycle-dual-body {
        display: flex;
        flex-direction: column;
        justify-content: center;
        gap: 8px;
        margin-top: auto;
        min-height: 72px;
    }
    .trade-cycle-dual-line {
        font-size: 0.92rem;
        line-height: 1.35;
        font-weight: 900;
        white-space: normal;
        overflow: visible;
        word-break: break-word;
        font-variant-numeric: tabular-nums;
    }
    .cycle-kv-card {
        box-sizing: border-box;
        border: 0;
        border-radius: 0;
        padding: 6px 2px;
        background: transparent;
        height: fit-content;
    }
    .cycle-kv-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
        padding: 9px 0;
        border-bottom: 1px solid rgba(148, 163, 184, 0.10);
    }
    .cycle-kv-row:last-child {
        border-bottom: none;
    }
    .cycle-kv-label {
        color: #94a3b8;
        font-size: 13px;
        font-weight: 800;
        letter-spacing: 0.04em;
    }
    .cycle-kv-value {
        max-width: 70%;
        color: #f8fafc;
        font-size: 14px;
        font-weight: 900;
        line-height: 1.4;
        text-align: right;
        font-variant-numeric: tabular-nums;
        word-break: break-word;
    }
    .cycle-flow-card {
        box-sizing: border-box;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 18px;
        min-height: 220px;
        padding: 8px 4px;
        border: 0;
        border-radius: 0;
        background: transparent;
    }
    .cycle-flow-endpoint {
        flex: 1 1 0;
        min-width: 0;
    }
    .cycle-flow-exit {
        text-align: right;
    }
    .cycle-flow-tag {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 6px 14px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 900;
        letter-spacing: 0.08em;
    }
    .cycle-flow-entry-tag {
        color: #fecaca;
        background: rgba(239, 68, 68, 0.16);
        border: 1px solid rgba(239, 68, 68, 0.25);
    }
    .cycle-flow-exit-tag {
        color: #bbf7d0;
        background: rgba(34, 197, 94, 0.12);
        border: 1px solid rgba(34, 197, 94, 0.24);
    }
    .cycle-flow-date {
        margin-top: 16px;
        color: #f8fafc;
        font-size: 1.35rem;
        font-weight: 900;
        letter-spacing: -0.04em;
        font-variant-numeric: tabular-nums;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .cycle-flow-price {
        margin-top: 10px;
        color: #38bdf8;
        font-size: 2rem;
        font-weight: 900;
        line-height: 1;
        letter-spacing: -0.05em;
        font-variant-numeric: tabular-nums;
    }
    .cycle-flow-center {
        flex: 0 0 auto;
        text-align: center;
    }
    .cycle-flow-arrow {
        color: #94a3b8;
        font-size: 3rem;
        line-height: 1;
        font-weight: 900;
    }
    .cycle-flow-days {
        margin-top: 10px;
        color: #cbd5e1;
        font-size: 14px;
        font-weight: 800;
        letter-spacing: 0.04em;
    }
    .cycle-flow-pnl {
        margin-top: 12px;
        font-size: 1.15rem;
        font-weight: 900;
        letter-spacing: -0.04em;
        font-variant-numeric: tabular-nums;
    }
    .cycle-flow-footnote {
        margin: 10px 2px 0;
        color: #cbd5e1;
        font-size: 13px;
        font-weight: 800;
        letter-spacing: 0.04em;
    }
    .dashboard-stat-card {
        position: relative;
        display: flex;
        flex-direction: column;
        justify-content: center;
        overflow: hidden;
        border: 0;
        border-radius: 0;
        padding: 8px 0;
        background: transparent;
        box-shadow: none;
        min-height: 120px;
    }
    .dashboard-stat-topline {
        display: none;
    }
    .dashboard-stat-label {
        color: #94a3b8;
        font-size: 0.82rem;
        font-weight: 800;
        letter-spacing: 0.08em;
        line-height: 1.35;
        min-height: 34px;
    }
    .dashboard-stat-value {
        max-width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        margin-top: 14px;
        font-size: clamp(1.55rem, 2.2vw, 2rem);
        line-height: 1.1;
        font-weight: 900;
        letter-spacing: -0.04em;
        white-space: nowrap;
        font-variant-numeric: tabular-nums;
    }
    .dashboard-stat-sub {
        margin-top: 8px;
        color: #cbd5e1;
        font-size: 0.9rem;
        font-weight: 600;
        line-height: 1.4;
    }
    .dashboard-hero-panel {
        box-sizing: border-box;
        min-height: 300px;
        padding: 14px 8px 10px 4px;
    }
    .dashboard-hero-label {
        color: #94a3b8;
        font-size: 0.78rem;
        font-weight: 900;
        letter-spacing: 0.14em;
    }
    .dashboard-hero-nav {
        margin-top: 16px;
        color: #f8fafc;
        font-size: clamp(3rem, 6vw, 4.8rem);
        line-height: 0.95;
        font-weight: 700;
        letter-spacing: -0.06em;
        font-variant-numeric: tabular-nums;
    }
    .dashboard-hero-divider {
        height: 1px;
        margin: 28px 0 24px;
        background: linear-gradient(90deg, rgba(56,189,248,0.45), rgba(148,163,184,0.12), transparent);
    }
    .dashboard-hero-row {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 18px;
    }
    .dashboard-hero-item {
        min-width: 0;
    }
    .dashboard-hero-item-label {
        color: #94a3b8;
        font-size: 0.8rem;
        font-weight: 800;
        letter-spacing: 0.08em;
    }
    .dashboard-hero-item-value {
        margin-top: 10px;
        font-size: clamp(1.45rem, 2.8vw, 2rem);
        line-height: 1;
        font-weight: 900;
        letter-spacing: -0.05em;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        font-variant-numeric: tabular-nums;
    }
    .dashboard-hero-item-sub {
        margin-top: 8px;
        font-size: 0.9rem;
        font-weight: 700;
        line-height: 1.35;
        color: #cbd5e1;
    }
    .dashboard-inline-strip {
        display: grid;
        grid-template-columns: repeat(5, minmax(0, 1fr));
        gap: 22px;
        margin-top: 26px;
        padding: 0 4px;
    }
    .dashboard-inline-metric {
        min-width: 0;
        padding-top: 14px;
        border-top: 1px solid rgba(148, 163, 184, 0.16);
    }
    .dashboard-inline-metric-head {
        display: flex;
        align-items: center;
        gap: 10px;
        min-width: 0;
    }
    .dashboard-strip-dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        box-shadow: 0 0 0 5px rgba(255, 255, 255, 0.04);
        flex-shrink: 0;
    }
    .dashboard-strip-label {
        color: #e5e7eb;
        font-size: 0.84rem;
        font-weight: 800;
        letter-spacing: 0.02em;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .dashboard-strip-sub {
        margin-top: 8px;
        color: #94a3b8;
        font-size: 0.82rem;
        line-height: 1.35;
        font-weight: 600;
    }
    .dashboard-strip-value {
        margin-top: 12px;
        font-size: 1.45rem;
        line-height: 1;
        font-weight: 900;
        letter-spacing: -0.05em;
        white-space: nowrap;
        font-variant-numeric: tabular-nums;
    }
    .allocation-ratio-card {
        border: 0;
        border-radius: 0;
        padding: 4px 2px 0;
        background: transparent;
        box-shadow: none;
        min-height: 520px;
    }
    .allocation-ratio-header,
    .allocation-ratio-row {
        display: grid;
        grid-template-columns: minmax(0, 1.45fr) 0.8fr 1fr;
        align-items: center;
        column-gap: 12px;
    }
    .allocation-ratio-header {
        padding: 4px 6px 14px;
        color: #94a3b8;
        font-size: 0.8rem;
        font-weight: 800;
        letter-spacing: 0.08em;
    }
    .allocation-ratio-row {
        padding: 12px 8px;
        border-top: 1px solid rgba(51, 65, 85, 0.68);
    }
    .allocation-asset-cell {
        display: flex;
        align-items: center;
        gap: 10px;
        min-width: 0;
    }
    .allocation-color-dot {
        width: 12px;
        height: 12px;
        border-radius: 999px;
        flex-shrink: 0;
        box-shadow: 0 0 0 4px rgba(255, 255, 255, 0.04);
    }
    .allocation-asset-name {
        color: #f8fafc;
        font-size: 0.95rem;
        font-weight: 700;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .allocation-weight-cell {
        color: #e5e7eb;
        font-size: 0.95rem;
        font-weight: 800;
        text-align: right;
        font-variant-numeric: tabular-nums;
    }
    .allocation-value-cell {
        color: #cbd5e1;
        font-size: 0.95rem;
        font-weight: 700;
        text-align: right;
        font-variant-numeric: tabular-nums;
    }
    div[data-testid="stTextArea"] textarea {
        background: transparent !important;
        border: 0 !important;
        box-shadow: none !important;
        color: #f8fafc !important;
        font-size: 1rem !important;
        line-height: 1.8 !important;
    }
    div[data-testid="stTextInput"] input {
        background: transparent !important;
        border: 0 !important;
        color: #f8fafc !important;
        font-size: 1rem !important;
    }
</style>
""",
    unsafe_allow_html=True,
)


st.title("📈 多帳戶波段交易與 AI 記帳系統")

try:
    get_supabase_client()
    ensure_db_schema()
    portfolios_df = get_portfolios()
except Exception as exc:
    st.error(
        f"Supabase 資料庫連線或初始化失敗：{exc}"
    )
    st.stop()

# --- App State & Navigation ---
app_mode = st.sidebar.radio("🧭 系統模式", ["💼 投資組合空間", "🌐 全域操盤筆記本"])
st.sidebar.markdown("---")

if app_mode == "🌐 全域操盤筆記本":
    st.header("全域操盤筆記本 (跨帳戶共用)")
    st.caption("第一區保留每日操盤日誌；第二區整理可反覆修訂的交易策略；第三區沉澱交易知識。")

    daily_tab, strategy_tab, knowledge_tab = st.tabs(
        ["📅 每日操盤筆記", "🧭 交易策略專區", "📚 交易知識庫"]
    )
    with daily_tab:
        render_daily_macro_journal_tab()
    with strategy_tab:
        render_article_notebook_tab(
            "strategy",
            "🧭 交易策略專區",
            "新增交易策略筆記",
            "strategy_note",
        )
    with knowledge_tab:
        render_article_notebook_tab(
            "knowledge",
            "📚 交易知識庫",
            "新增交易知識筆記",
            "knowledge_note",
        )

else:
    # --- Sidebar Setup & Portfolios ---
    with st.sidebar:
        st.header("💼 資金池與狀態")

        with st.expander("➕ 新增資金池 (期初建檔)"):
            st.markdown("**(可選) 🤖 AI 影像辨識快速匯入庫存**")
            port_file = st.file_uploader(
                "上傳券商庫存總覽截圖", type=["png", "jpg", "jpeg"], key="port_uploader"
            )
            if port_file is not None:
                if st.button("AI 辨識持股", use_container_width=True):
                    with st.spinner("Gemini Vision 辨識庫存中..."):
                        image_bytes = port_file.read()
                        res_list = ai_vision_portfolio(image_bytes)
                        if res_list is not None:
                            st.session_state["init_portfolio_df"] = pd.DataFrame(res_list)
                            st.success(f"✅ 辨識完成！共識別 {len(res_list)} 檔持股，請在下方表單確認或修改")

            st.markdown("---")
            with st.form("new_port_form"):
                new_name = st.text_input("資金池名稱")
                start_t0 = st.number_input("初始 T+0 現金", value=100000.0)
                start_date = st.date_input(
                    "建檔日期", datetime.now() - timedelta(days=1)
                )
                st.markdown("**請輸入或確認持股資訊**")

                if "init_portfolio_df" not in st.session_state:
                    init_df = pd.DataFrame(
                        [{"股票代碼 (純代號)": "", "持股均價": 0.0, "持有股數": 0}]
                    )
                else:
                    init_df = st.session_state["init_portfolio_df"]

                edited_df = st.data_editor(init_df, num_rows="dynamic", hide_index=True)

                if st.form_submit_button("建立資金池"):
                    if not new_name.strip():
                        st.error("資金池名稱不能為空字串！")
                    else:
                        try:
                            new_id = create_portfolio(
                                new_name.strip(), initial_cash=0.0
                            )
                            if float(start_t0 or 0) > 0:
                                execute_cashflow(
                                    new_id,
                                    start_date.strftime("%Y-%m-%d"),
                                    "Deposit",
                                    start_t0,
                                )

                            # 迴圈讀取所有輸入的期初持股
                            for index, row in edited_df.iterrows():
                                s_code_raw = row.get("股票代碼 (純代號)")
                                s_code = (
                                    normalize_stock_id(s_code_raw)
                                    if pd.notna(s_code_raw) and s_code_raw is not None
                                    else ""
                                )

                                s_price_raw = row.get("持股均價")
                                s_price = (
                                    float(s_price_raw)
                                    if pd.notna(s_price_raw) and s_price_raw is not None
                                    else 0.0
                                )

                                s_shares_raw = row.get("持有股數")
                                s_shares = (
                                    int(s_shares_raw)
                                    if pd.notna(s_shares_raw)
                                    and s_shares_raw is not None
                                    else 0
                                )

                                if s_code and s_shares > 0:
                                    execute_trade(
                                        new_id,
                                        start_date.strftime("%Y-%m-%d"),
                                        s_code,
                                        "Setup",
                                        s_price,
                                        s_shares,
                                        False,
                                        0,
                                        0,
                                        0,
                                        0,
                                        "期初庫存建檔",
                            )

                            invalidate_portfolio_runtime_bundle()
                            show_ui_toast(f"資金池 {new_name.strip()} 已建立")
                            if "init_portfolio_df" in st.session_state:
                                del st.session_state["init_portfolio_df"]
                            st.rerun()
                        except sqlite3.IntegrityError:
                            st.error(f"資金池名稱 '{new_name}' 已存在，請換一個名字。")

        if portfolios_df.empty:
            st.warning("請先上方新增第一個資金池！")
            st.stop()

        official_nav_date = get_latest_official_tw_trading_date()
        nav_sync_signature = (
            f"{official_nav_date}|"
            + ",".join(str(pid) for pid in portfolios_df["id"].tolist())
        )
        if st.session_state.get("official_nav_sync_signature") != nav_sync_signature:
            with st.spinner("同步收盤後正式 NAV 中..."):
                nav_sync_result = ensure_portfolios_official_nav_synced(
                    portfolios_df["id"].tolist(),
                    target_date=official_nav_date,
                )
            if (
                int(nav_sync_result.get("synced", 0) or 0) > 0
                or nav_sync_result.get("holiday_deleted_dates")
            ):
                invalidate_portfolio_runtime_bundle()
            st.session_state["official_nav_sync_signature"] = nav_sync_signature

        portfolios_df = portfolios_df.sort_values("id", kind="stable").reset_index(
            drop=True
        )
        p_dict = dict(zip(portfolios_df["name"], portfolios_df["id"]))
        portfolio_options = list(p_dict.keys())
        if st.session_state.get("selected_portfolio_name") not in portfolio_options:
            st.session_state["selected_portfolio_name"] = portfolio_options[0]
        selected_p_name = st.selectbox(
            "選擇操作資金池",
            options=portfolio_options,
            key="selected_portfolio_name",
        )
        current_pid = p_dict[selected_p_name]

        t0_cash, t2_cash = get_portfolio_state(current_pid)
        st.markdown(
            f"""
            <div class="metric-card">
                <div>總購買力 (T+0)</div>
                <div class="metric-value">${t0_cash:,.0f}</div>
            </div>
        """,
            unsafe_allow_html=True,
        )

        st.markdown("<br><br>", unsafe_allow_html=True)
        with st.expander("⚙️ 資金池管理 (危險操作)"):
            st.warning("⚠️ 此操作將永久刪除所有相關紀錄且無法復原！")
            del_confirm = st.checkbox(f"我確定要刪除「{selected_p_name}」")
            if st.button(
                "🚨 徹底刪除目前資金池",
                disabled=not del_confirm,
                use_container_width=True,
            ):
                delete_portfolio_and_related_data(current_pid)
                invalidate_portfolio_runtime_bundle()
                show_ui_toast(f"資金池 {selected_p_name} 已刪除")
                st.rerun()

    # --- Top Action Bar ---
    top_col1, top_col2 = st.columns([3, 1])
    with top_col1:
        st.header(f"💼 投資組合總覽 - {selected_p_name}")
    with top_col2:
        if st.button("➕ 新增交易 / AI 辨識", use_container_width=True):
            st.session_state["editing_trade_id"] = None
            st.session_state["show_trade_entry_dialog"] = True

    if st.session_state.get("show_trade_entry_dialog") and not st.session_state.get("editing_trade_id"):
        render_trade_entry_dialog(current_pid)

    render_portfolio_refresh_notice(current_pid)

    st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    # --- Main Tabs ---
    runtime_bundle = get_portfolio_runtime_bundle(current_pid, official_nav_date)
    t0_v = runtime_bundle["t0_cash"]
    t2_v = runtime_bundle["t2_cash"]
    hist_df = runtime_bundle["hist_df"]
    current_twr = runtime_bundle["current_twr"]
    holdings_df = runtime_bundle["holdings_df"]

    if holdings_df.empty:
        pie_data = pd.DataFrame({"Asset": ["可用現金 (T+0)"], "Value": [t0_v]})
        total_holdings_mv = 0
    else:
        assets = ["可用現金 (T+0)"] + holdings_df["標的名稱"].tolist()
        values = [t0_v] + holdings_df["總市值"].tolist()
        pie_data = pd.DataFrame({"Asset": assets, "Value": values})
        total_holdings_mv = sum(holdings_df["總市值"].tolist())

    exact_nav = t0_v + total_holdings_mv
    pending_trades, pending_summary = build_pending_settlement_schedule(current_pid)
    pending_val = pending_summary["淨交割額"].sum() if not pending_summary.empty else 0.0
    next_settlement = pending_summary.iloc[0] if not pending_summary.empty else None

    if not hist_df.empty and len(hist_df) >= 2:
        nav_latest = float(hist_df.iloc[-1]["NAV"])
        nav_prev = float(hist_df.iloc[-2]["NAV"])
        day_delta = nav_latest - nav_prev
        day_delta_pct = (day_delta / nav_prev * 100) if nav_prev != 0 else 0
    else:
        day_delta = 0.0
        day_delta_pct = 0.0

    if not hist_df.empty:
        nav_latest = float(hist_df.iloc[-1]["NAV"])
        net_invested_amount = float(get_portfolio_net_invested_amount(current_pid) or 0.0)
        total_delta_pct = float(current_twr)
        total_delta = nav_latest - net_invested_amount
        latest_drawdown = float(hist_df.iloc[-1].get("Drawdown", 0) or 0)
        max_drawdown = (
            float(hist_df["Drawdown"].min())
            if "Drawdown" in hist_df.columns and not hist_df["Drawdown"].dropna().empty
            else 0.0
        )
        latest_cum_alpha = float(hist_df.iloc[-1].get("CumAlpha", 0) or 0)
        recent_perf_window = hist_df.tail(min(20, len(hist_df)))
        recent_win_rate = (
            (recent_perf_window["DailyAlpha"] > 0).mean() * 100
            if "DailyAlpha" in recent_perf_window.columns and not recent_perf_window.empty
            else 0.0
        )
        benchmark_total_pct = float(hist_df.iloc[-1].get("BenchmarkCumReturn", 0) or 0)
        metric_hist_df = hist_df.copy()
        if "IsTradingDay" in metric_hist_df.columns:
            trading_metric_df = metric_hist_df[
                metric_hist_df["IsTradingDay"] == True
            ].copy()
            if not trading_metric_df.empty:
                metric_hist_df = trading_metric_df
        daily_return_series = pd.to_numeric(
            metric_hist_df.get("DailyReturn", pd.Series(dtype=float)),
            errors="coerce",
        ).dropna()
        annualized_volatility = (
            daily_return_series.std(ddof=0) * np.sqrt(252)
            if len(daily_return_series) >= 2
            else 0.0
        )
        outperform_days_ratio = (
            (
                pd.to_numeric(metric_hist_df["DailyAlpha"], errors="coerce").fillna(0.0)
                > 0
            ).mean()
            * 100
            if "DailyAlpha" in metric_hist_df.columns and not metric_hist_df.empty
            else 0.0
        )
    else:
        total_delta = 0.0
        total_delta_pct = 0.0
        benchmark_total_pct = 0.0
        latest_drawdown = 0.0
        max_drawdown = 0.0
        latest_cum_alpha = 0.0
        recent_win_rate = 0.0
        annualized_volatility = 0.0
        outperform_days_ratio = 0.0

    def tw_profit_color(v):
        if v > 0:
            return "#ef4444"
        if v < 0:
            return "#22c55e"
        return "#e5e7eb"

    tab1, tab3, tab2, tab4 = st.tabs(
        ["📊 總合儀表板", "📦 持倉分析中心", "📝 交易回顧中心", "💰 資金出入"]
    )

    with tab1:
        st.subheader("核心資產概覽")
        render_dashboard_hero_panel(
            exact_nav,
            day_delta,
            day_delta_pct,
            total_delta,
            total_delta_pct,
            total_holdings_mv,
        )
        render_dashboard_metric_strip(
            [
                {
                    "label": "累積報酬（基準 0050）",
                    "value": f"{benchmark_total_pct:+.2f}%",
                    "value_color": tw_profit_color(benchmark_total_pct),
                    "sub_text": "Benchmark 累積績效",
                    "accent_color": "#22d3ee",
                },
                {
                    "label": "累積 Alpha（你 - 基準）",
                    "value": f"{latest_cum_alpha:+.2f}%",
                    "value_color": tw_profit_color(latest_cum_alpha),
                    "sub_text": "策略相對 0050 超額報酬",
                    "accent_color": "#f59e0b",
                },
                {
                    "label": "最大回撤",
                    "value": f"{max_drawdown:.2f}%",
                    "value_color": "#22c55e" if max_drawdown < 0 else "#f8fafc",
                    "sub_text": f"目前回撤 {latest_drawdown:.2f}%",
                    "accent_color": "#22c55e",
                },
                {
                    "label": "波動率（年化）",
                    "value": f"{annualized_volatility:.2f}%",
                    "value_color": "#f8fafc",
                    "sub_text": "由每日報酬率年化估算",
                    "accent_color": "#a78bfa",
                },
                {
                    "label": "跑贏天數比例",
                    "value": f"{outperform_days_ratio:.1f}%",
                    "value_color": "#38bdf8",
                    "sub_text": "每日 Alpha > 0 的交易日占比",
                    "accent_color": "#38bdf8",
                },
            ]
        )

        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        st.subheader("📈 總資產走勢（績效 %）")
        if not hist_df.empty:
            chart_df = hist_df.copy()
            if "IsTradingDay" in chart_df.columns:
                trading_df = chart_df[chart_df["IsTradingDay"] == True].copy()
                if not trading_df.empty:
                    chart_df = trading_df

            chart_df["日期"] = pd.to_datetime(chart_df["Date"], errors="coerce")
            chart_df = chart_df.dropna(subset=["日期"]).sort_values("日期")
            chart_df = chart_df.drop_duplicates(subset=["日期"], keep="last")
            chart_df = chart_df.reset_index(drop=True)
            for col in ["TWR", "BenchmarkCumReturn", "CumAlpha", "NAV"]:
                if col not in chart_df.columns:
                    chart_df[col] = np.nan
                chart_df[col] = pd.to_numeric(chart_df[col], errors="coerce")

            if chart_df["TWR"].notna().sum() > 0:
                chart_df["策略累積報酬"] = chart_df["TWR"].ffill().fillna(0.0)
            else:
                base_nav = (
                    chart_df["NAV"].dropna().iloc[0]
                    if chart_df["NAV"].notna().any()
                    else 0
                )
                if base_nav > 0:
                    chart_df["策略累積報酬"] = (
                        chart_df["NAV"] / base_nav - 1.0
                    ) * 100
                else:
                    chart_df["策略累積報酬"] = 0.0

            chart_df["0050累積報酬"] = chart_df["BenchmarkCumReturn"].ffill().fillna(0.0)
            chart_df["累積Alpha"] = chart_df["CumAlpha"].ffill().fillna(0.0)

            fig2 = go.Figure()
            fig2.add_trace(
                go.Scatter(
                    x=chart_df["日期"].dt.strftime("%Y-%m-%d"),
                    y=chart_df["策略累積報酬"],
                    name="策略累積報酬",
                    line=dict(color="#ef4444", width=3),
                    mode="lines",
                )
            )
            fig2.add_trace(
                go.Scatter(
                    x=chart_df["日期"].dt.strftime("%Y-%m-%d"),
                    y=chart_df["0050累積報酬"],
                    name="0050 累積報酬",
                    line=dict(color="#22d3ee", width=2.6, dash="dot"),
                    mode="lines",
                )
            )
            if chart_df["累積Alpha"].notna().sum() > 0:
                fig2.add_trace(
                    go.Scatter(
                        x=chart_df["日期"].dt.strftime("%Y-%m-%d"),
                        y=chart_df["累積Alpha"],
                        name="累積 Alpha",
                        line=dict(color="#f59e0b", width=2.2),
                        mode="lines",
                    )
                )
            fig2.add_hline(y=0, line_color="#475569", line_dash="dash", line_width=1)

            fig2.update_xaxes(
                type="category",
                title="交易日",
                tickfont=dict(size=10),
                tickangle=-45,
            )
            fig2.update_yaxes(
                title="累積績效 (%)",
                ticksuffix="%",
                zeroline=True,
                zerolinecolor="#475569",
            )
            fig2.update_layout(
                template="plotly_dark",
                margin=dict(t=20, b=20, l=10, r=10),
                height=520,
                legend=dict(orientation="h", y=1.02, x=0),
                hovermode="x unified",
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("尚未有足夠歷史資料繪製圖表")

        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        st.subheader("📊 資產配置比例")
        cash_weight_row = pd.DataFrame(
            [
                {
                    "標的名稱": "可用現金 (T+0)",
                    "總市值": float(t0_v),
                }
            ]
        )
        allocation_df = (
            holdings_df[["標的名稱", "總市值"]].copy()
            if not holdings_df.empty
            else pd.DataFrame(columns=["標的名稱", "總市值"])
        )
        allocation_df = pd.concat([cash_weight_row, allocation_df], ignore_index=True)
        allocation_df = allocation_df.sort_values("總市值", ascending=False).reset_index(
            drop=True
        )
        alloc_mv_sum = allocation_df["總市值"].sum()
        allocation_df["配置比例(%)"] = (
            allocation_df["總市值"] / alloc_mv_sum * 100 if alloc_mv_sum > 0 else 0
        )
        allocation_colors = px.colors.qualitative.Prism
        allocation_color_map = {
            asset_name: allocation_colors[idx % len(allocation_colors)]
            for idx, asset_name in enumerate(allocation_df["標的名稱"].tolist())
        }

        alloc_chart_col, alloc_table_col = st.columns([1.5, 0.95])
        with alloc_chart_col:
            fig1 = px.pie(
                allocation_df,
                values="總市值",
                names="標的名稱",
                hole=0.34,
                color="標的名稱",
                color_discrete_map=allocation_color_map,
            )
            fig1.update_traces(
                textposition="inside",
                textinfo="percent",
                textfont_size=15,
                hovertemplate="%{label}<br>配置比例 %{percent}<br>總市值 $%{value:,.0f}<extra></extra>",
                showlegend=False,
            )
            fig1.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=10, b=10, l=10, r=10),
                height=620,
                showlegend=False,
            )
            st.plotly_chart(fig1, use_container_width=True)

        with alloc_table_col:
            render_allocation_ratio_table(allocation_df, allocation_color_map)

        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        with st.expander("⏳ 交割與現金明細", expanded=False):
            s1, s2, s3 = st.columns(3)
            with s1:
                render_big_value_card(
                    "可用預算 T+0",
                    f"${t0_v:,.0f}",
                    "#f8fafc",
                    "可立即使用資金",
                    card_class="holdings-stat-card",
                )
            with s2:
                render_big_value_card(
                    "銀行結餘 T+2",
                    f"${t2_v:,.0f}",
                    "#f8fafc",
                    "含待交割後餘額",
                    card_class="holdings-stat-card",
                )
            with s3:
                render_big_value_card(
                    "待交割淨額",
                    f"${pending_val:,.0f}",
                    tw_profit_color(pending_val),
                    "正數應收 / 負數應付",
                    card_class="holdings-stat-card",
                )

            if next_settlement is not None:
                next_net = float(next_settlement["淨交割額"])
                next_desc = (
                    f"應收 ${abs(next_net):,.0f}"
                    if next_net > 0
                    else (f"應付 ${abs(next_net):,.0f}" if next_net < 0 else "持平 $0")
                )
                st.caption(
                    f"下一個交割日：{next_settlement['預計交割日']}｜當日淨交割：{next_desc}"
                )
            else:
                st.caption("目前沒有待交割交易。")

            with st.popover("🗓️ 休市日曆", use_container_width=False):
                st.caption("TWSE 年度休市會自動同步；此處也可手動補特殊停市日。")
                if st.button(
                    "🔄 從 TWSE 同步年度休市",
                    key=f"holiday_sync_twse_{current_pid}",
                    use_container_width=True,
                ):
                    synced_count = sync_twse_market_holidays()
                    invalidate_portfolio_runtime_bundle()
                    show_ui_toast(f"已同步 {synced_count} 筆 TWSE 休市資料")
                    st.rerun()

                hol_df = get_market_holidays_df()
                add_h_col1, add_h_col2 = st.columns([1.2, 1.8])
                h_date = add_h_col1.date_input(
                    "日期",
                    value=datetime.now(),
                    key=f"holiday_add_date_{current_pid}",
                )
                h_reason = add_h_col2.text_input(
                    "原因",
                    value="",
                    key=f"holiday_add_reason_{current_pid}",
                    placeholder="例：補假、颱風停市",
                )
                h_settlement_open = st.checkbox(
                    "這天休市但仍可辦理交割",
                    value=False,
                    key=f"holiday_settlement_open_{current_pid}",
                )
                if st.button("新增休市日", key=f"holiday_add_btn_{current_pid}"):
                    upsert_market_holiday(
                        h_date.strftime("%Y-%m-%d"),
                        h_reason.strip(),
                        h_settlement_open,
                    )
                    invalidate_portfolio_runtime_bundle()
                    show_ui_toast("休市日已新增")
                    st.rerun()

                if not hol_df.empty:
                    del_target = st.selectbox(
                        "刪除日期",
                        options=hol_df["date"].tolist(),
                        index=len(hol_df) - 1,
                        key=f"holiday_del_target_{current_pid}",
                    )
                    if st.button("刪除", key=f"holiday_del_btn_{current_pid}"):
                        delete_market_holiday(del_target)
                        invalidate_portfolio_runtime_bundle()
                        show_ui_toast("休市日已刪除")
                        st.rerun()
                    st.dataframe(
                        hol_df.tail(5),
                        use_container_width=True,
                        hide_index=True,
                        height=170,
                    )

            if not pending_summary.empty:
                st.dataframe(
                    pending_summary[
                        ["預計交割日", "交割方向", "淨交割額", "筆數"]
                    ].style.format({"淨交割額": "${:,.0f}"}),
                    use_container_width=True,
                    hide_index=True,
                )

                if st.toggle("顯示交割交易明細", value=False):
                    st.dataframe(
                        pending_trades[
                            [
                                "交易日",
                                "預計交割日",
                                "標的",
                                "動作",
                                "交割方向",
                                "交割金額",
                            ]
                        ].style.format({"交割金額": "${:,.0f}"}),
                        use_container_width=True,
                        hide_index=True,
                    )

    with tab3:
        st.header("持倉分析中心")
        if not holdings_df.empty:
            suspect_mask = (
                holdings_df["當前現價"].isna()
                | (holdings_df["當前現價"] <= 0)
                | holdings_df["報價來源"].isin(["無即時報價", "估算報價"])
            )
            quote_counts = holdings_df["報價來源"].fillna("未知").value_counts().to_dict()
            quote_summary = "｜".join(
                [f"{src} {cnt} 檔" for src, cnt in quote_counts.items()]
            )
            st.caption(f"目前報價檢查：{quote_summary}")

            suspect_df = holdings_df[suspect_mask].copy()
            if not suspect_df.empty:
                st.warning("偵測到部分標的報價可能不穩定，建議手動填入今日現價確認。")
                st.dataframe(
                    suspect_df[["標的", "標的名稱", "平均成本", "當前現價"]].style.format(
                        {"平均成本": "${:,.1f}", "當前現價": "${:,.1f}"}
                    ),
                    use_container_width=True,
                    hide_index=True,
                    height=180,
                )

            with st.popover("✍️ 手動填入今日現價", use_container_width=False):
                st.caption("可覆蓋抓價結果；清空或填 0 即取消覆蓋。")
                manual_edit_df = holdings_df[
                    ["標的", "標的名稱", "當前現價", "手動覆蓋價", "報價來源"]
                ].copy()
                edited_manual_df = st.data_editor(
                    manual_edit_df,
                    hide_index=True,
                    disabled=["標的", "標的名稱", "當前現價", "報價來源"],
                    column_config={
                        "當前現價": st.column_config.NumberColumn(format="$%.1f"),
                        "手動覆蓋價": st.column_config.NumberColumn(
                            help="填入後會優先取代自動報價；清空或填 0 代表取消覆蓋",
                            format="$%.1f",
                        ),
                    },
                    key=f"manual_price_editor_{current_pid}",
                )

                if st.button("儲存手動現價", key=f"save_manual_price_{current_pid}"):
                    for _, row in edited_manual_df.iterrows():
                        sid = normalize_stock_id(row["標的"])
                        override_val = row.get("手動覆蓋價", np.nan)
                        if pd.notna(override_val) and float(override_val) > 0:
                            set_manual_price_override(sid, float(override_val))
                        else:
                            delete_manual_price_override(sid)
                    invalidate_portfolio_runtime_bundle(current_pid)
                    show_ui_toast("已更新手動現價覆蓋設定")
                    st.rerun()

            holdings_num = len(holdings_df)
            total_mv = float(holdings_df["總市值"].sum())
            total_unrealized = float(holdings_df["未實現損益 ($)"].sum())
            win_ratio = (
                (holdings_df["未實現損益 ($)"] > 0).sum() / holdings_num * 100
                if holdings_num > 0
                else 0
            )

            hm1, hm2, hm3, hm4 = st.columns(4)
            with hm1:
                render_big_value_card(
                    "持倉檔數",
                    f"{holdings_num} 檔",
                    "#f8fafc",
                    "目前仍持有的股票數",
                    "holdings-stat-card",
                )
            with hm2:
                render_big_value_card(
                    "持倉總市值",
                    f"${total_mv:,.0f}",
                    "#38bdf8",
                    "依當前報價計算",
                    "holdings-stat-card",
                )
            with hm3:
                render_big_value_card(
                    "未實現總損益",
                    f"${total_unrealized:+,.0f}",
                    tw_profit_color(total_unrealized),
                    "紅色獲利 / 綠色虧損",
                    "holdings-stat-card",
                )
            with hm4:
                render_big_value_card(
                    "持倉獲利比率",
                    f"{win_ratio:.1f}%",
                    "#f59e0b" if win_ratio >= 50 else "#f8fafc",
                    "獲利部位占全部持股",
                    "holdings-stat-card",
                )

            st.subheader("持有天數 × 報酬效率矩陣")
            efficiency_df = holdings_df[
                [
                    "標的名稱",
                    "持有天數",
                    "報酬效率 (%/日)",
                    "未實現報酬率 (%)",
                    "總市值",
                    "停利目標價",
                    "停損目標價",
                    "距停利 (%)",
                    "距停損 (%)",
                ]
            ].copy()
            efficiency_df["持有天數"] = pd.to_numeric(
                efficiency_df["持有天數"], errors="coerce"
            ).fillna(0)
            efficiency_df["報酬效率 (%/日)"] = pd.to_numeric(
                efficiency_df["報酬效率 (%/日)"], errors="coerce"
            ).fillna(0.0)
            efficiency_df["總市值"] = pd.to_numeric(
                efficiency_df["總市值"], errors="coerce"
            ).fillna(0.0)

            if len(efficiency_df) > 0:
                fig_eff = px.scatter(
                    efficiency_df,
                    x="持有天數",
                    y="報酬效率 (%/日)",
                    size=efficiency_df["總市值"].abs().clip(lower=1),
                    color="未實現報酬率 (%)",
                    text="標的名稱",
                    hover_data={
                        "標的名稱": True,
                        "持有天數": True,
                        "報酬效率 (%/日)": ":.3f",
                        "未實現報酬率 (%)": ":.2f",
                        "總市值": ":,.0f",
                        "停利目標價": ":,.2f",
                        "停損目標價": ":,.2f",
                        "距停利 (%)": ":.2f",
                        "距停損 (%)": ":.2f",
                    },
                    color_continuous_scale=[[0, "#22c55e"], [0.5, "#f8fafc"], [1, "#ef4444"]],
                    title="越往右代表持有越久，越往上代表每日報酬效率越高",
                )
                fig_eff.update_traces(textposition="top center", marker=dict(line=dict(width=1, color="#0f172a")))
                fig_eff.add_hline(y=0, line_dash="dash", line_color="#475569", line_width=1)
                fig_eff.update_layout(
                    template="plotly_dark",
                    height=460,
                    margin=dict(t=60, b=20, l=10, r=10),
                    coloraxis_colorbar=dict(title="未實現報酬%"),
                )
                fig_eff.update_xaxes(
                    title="持有天數",
                    dtick=1,
                    tick0=0,
                    rangemode="tozero",
                )
                fig_eff.update_yaxes(title="報酬效率 (%/日)", ticksuffix="%")
                st.plotly_chart(fig_eff, use_container_width=True)

            def color_pnl(val):
                try:
                    num = float(val)
                    color = "#ef4444" if num > 0 else "#22c55e" if num < 0 else "white"
                except Exception:
                    color = "white"
                return f"color: {color}"

            st.subheader("庫存與未實現損益明細")
            inventory_df = holdings_df[
                [
                    "標的",
                    "標的名稱",
                    "持有起始日",
                    "持有天數",
                    "平均成本",
                    "當前現價",
                    "停利目標價",
                    "距停利 (%)",
                    "停損目標價",
                    "距停損 (%)",
                    "總市值",
                    "未實現損益 ($)",
                    "未實現報酬率 (%)",
                    "報酬效率 (%/日)",
                ]
            ].copy()
            st.dataframe(
                inventory_df.style.format(
                    {
                        "平均成本": "${:,.1f}",
                        "當前現價": "${:,.1f}",
                        "停利目標價": "${:,.2f}",
                        "距停利 (%)": "{:+.2f}%",
                        "停損目標價": "${:,.2f}",
                        "距停損 (%)": "{:+.2f}%",
                        "總市值": "${:,.1f}",
                        "未實現損益 ($)": "${:,.1f}",
                        "未實現報酬率 (%)": "{:+.2f}%",
                        "報酬效率 (%/日)": "{:+.3f}%",
                    },
                    na_rep="-",
                ).map(
                    color_pnl,
                    subset=[
                        "距停利 (%)",
                        "距停損 (%)",
                        "未實現損益 ($)",
                        "未實現報酬率 (%)",
                        "報酬效率 (%/日)",
                    ],
                ),
                use_container_width=True,
                hide_index=True,
                height=360,
            )
            with st.popover("🎯 編輯庫存停利 / 停損", use_container_width=False):
                risk_edit_df = holdings_df[
                    ["標的", "標的名稱", "當前現價", "停利目標價", "停損目標價"]
                ].copy()
                edited_inventory_df = st.data_editor(
                    risk_edit_df,
                    use_container_width=True,
                    hide_index=True,
                    disabled=["標的", "標的名稱", "當前現價"],
                    column_config={
                        "當前現價": st.column_config.NumberColumn(format="$%.1f"),
                        "停利目標價": st.column_config.NumberColumn(
                            format="$%.2f",
                            help="直接輸入新的停利價格；清空或填 0 代表取消停利",
                        ),
                        "停損目標價": st.column_config.NumberColumn(
                            format="$%.2f",
                            help="直接輸入新的停損價格；清空或填 0 代表取消停損",
                        ),
                    },
                    key=f"holdings_risk_editor_{current_pid}",
                )
                if st.button(
                    "儲存庫存停利 / 停損設定",
                    use_container_width=True,
                    key=f"save_holdings_risk_{current_pid}",
                ):
                    changed_count = 0
                    for _, edited_row in edited_inventory_df.iterrows():
                        sid = normalize_stock_id(edited_row.get("標的", ""))
                        base_row = risk_edit_df[risk_edit_df["標的"] == sid]
                        if base_row.empty:
                            continue
                        base_row = base_row.iloc[0]
                        edited_tp = edited_row.get("停利目標價")
                        edited_sl = edited_row.get("停損目標價")
                        if _is_price_target_changed(
                            base_row.get("停利目標價"),
                            edited_tp,
                        ) or _is_price_target_changed(
                            base_row.get("停損目標價"),
                            edited_sl,
                        ):
                            update_holding_risk_targets(
                                current_pid,
                                sid,
                                take_profit_price=edited_tp,
                                stop_loss_price=edited_sl,
                            )
                            changed_count += 1
                    if changed_count:
                        invalidate_portfolio_runtime_bundle(current_pid)
                        show_ui_toast(f"已更新 {changed_count} 檔持倉的停利 / 停損設定")
                        st.rerun()
                    else:
                        st.info("目前沒有偵測到停利 / 停損設定變更。")
        else:
            st.info("目前無持股，無法產生持倉分析。")

        st.markdown("---")
        st.subheader("每日淨值與績效明細 (Daily NAV History)")
        if not hist_df.empty:
            nav_vis = hist_df.copy()
            if "IsTradingDay" in nav_vis.columns:
                trading_vis = nav_vis[nav_vis["IsTradingDay"] == True].copy()
                if not trading_vis.empty:
                    nav_vis = trading_vis
            for col in [
                "DailyReturn",
                "TWR",
                "BenchmarkCumReturn",
                "BenchmarkPrice",
                "BenchmarkDailyReturn",
                "CumAlpha",
                "DailyAlpha",
                "Drawdown",
                "Notes",
                "Win?",
                "NAV",
                "CashFlow",
            ]:
                if col not in nav_vis.columns:
                    nav_vis[col] = "" if col in ["Notes", "Win?"] else 0.0
            nav_vis["日期"] = pd.to_datetime(nav_vis["Date"], errors="coerce")
            nav_vis = nav_vis.dropna(subset=["日期"]).sort_values("日期")
            nav_vis = nav_vis.drop_duplicates(subset=["日期"], keep="last").reset_index(
                drop=True
            )
            if nav_vis.empty:
                st.info("目前無有效日期資料，先以預設列顯示。")
                nav_vis = pd.DataFrame(
                    {
                        "日期": [pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))],
                        "DailyReturn": [0.0],
                        "TWR": [0.0],
                        "BenchmarkCumReturn": [0.0],
                        "CumAlpha": [0.0],
                        "DailyAlpha": [0.0],
                        "Drawdown": [0.0],
                        "Notes": [""],
                        "Win?": ["-"],
                        "NAV": [0.0],
                        "CashFlow": [0.0],
                    }
                )

            for col in [
                "DailyReturn",
                "TWR",
                "BenchmarkCumReturn",
                "BenchmarkPrice",
                "BenchmarkDailyReturn",
                "CumAlpha",
                "DailyAlpha",
                "Drawdown",
                "NAV",
                "CashFlow",
            ]:
                nav_vis[col] = pd.to_numeric(nav_vis[col], errors="coerce").fillna(0.0)

            nav_vis["日期字串"] = nav_vis["日期"].dt.strftime("%Y-%m-%d")
            nav_vis["日損益"] = nav_vis["NAV"].diff().fillna(0)
            nav_vis["日報酬率(%)"] = nav_vis["DailyReturn"]
            nav_vis["累積 TWR(%)"] = nav_vis["TWR"]
            nav_vis["0050累積(%)"] = nav_vis["BenchmarkCumReturn"]
            nav_vis["累積Alpha(%)"] = nav_vis["CumAlpha"]
            nav_vis["日Alpha(%)"] = nav_vis["DailyAlpha"]
            nav_vis["回撤(%)"] = nav_vis["Drawdown"]
            nav_vis["累積指數(=1起)"] = 1 + nav_vis["累積 TWR(%)"] / 100
            nav_vis["累積高點"] = nav_vis["累積指數(=1起)"].cummax()
            nav_vis["基準累積指數"] = 1 + nav_vis["0050累積(%)"] / 100

            def prettify_note(raw_note, cashflow):
                text = "" if pd.isna(raw_note) else str(raw_note).strip()
                replace_map = {
                    "Buy": "買進",
                    "Add": "加碼",
                    "Reduce": "減碼",
                    "Close": "出清",
                    "Setup": "建倉",
                    ",": "｜",
                }
                for k, v in replace_map.items():
                    text = text.replace(k, v)

                cf_hint = ""
                if cashflow > 0:
                    cf_hint = f"外部入金 ${cashflow:,.0f}"
                elif cashflow < 0:
                    cf_hint = f"外部出金 ${abs(cashflow):,.0f}"

                if text and cf_hint:
                    return f"{cf_hint}；{text}"
                if text:
                    return text
                if cf_hint:
                    return cf_hint
                return "-"

            nav_vis["操作摘要"] = nav_vis.apply(
                lambda r: prettify_note(r.get("Notes", ""), float(r.get("CashFlow", 0.0))),
                axis=1,
            )
            nav_vis["勝負"] = nav_vis["Win?"].replace({"Y": "勝", "N": "負", "-": "平"})

            latest_nav_row = nav_vis.iloc[-1]
            k1, k2, k3 = st.columns(3)
            with k1:
                render_big_value_card(
                    f"{latest_nav_row['日期字串']} 投資資產淨值",
                    f"${float(latest_nav_row['NAV']):,.0f}",
                    "#38bdf8",
                    f"當日損益 {float(latest_nav_row['日損益']):+,.0f}",
                )
            with k2:
                render_big_value_card(
                    "當日報酬 / 累積 TWR",
                    f"{float(latest_nav_row['日報酬率(%)']):+.2f}% / {float(latest_nav_row['累積 TWR(%)']):+.2f}%",
                    tw_profit_color(float(latest_nav_row["日報酬率(%)"])),
                    f"目前回撤 {float(latest_nav_row['回撤(%)']):.2f}%",
                )
            with k3:
                render_big_value_card(
                    "0050 價格 / 累積 Alpha",
                    f"${float(latest_nav_row['BenchmarkPrice']):,.2f} / {float(latest_nav_row['累積Alpha(%)']):+.2f}%",
                    "#22d3ee",
                    f"0050 累積報酬 {float(latest_nav_row['0050累積(%)']):+.2f}%",
                )
            st.caption(f"最近交易日摘要：{latest_nav_row['操作摘要']}")

            chart_df = nav_vis.copy()
            chart_df = chart_df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
            chart_df["累積 TWR(%)"] = pd.to_numeric(
                chart_df["累積 TWR(%)"], errors="coerce"
            ).fillna(0.0)
            chart_df["0050累積(%)"] = pd.to_numeric(
                chart_df["0050累積(%)"], errors="coerce"
            ).fillna(0.0)

            if len(chart_df) >= 2:
                y_min = min(chart_df["累積 TWR(%)"].min(), chart_df["0050累積(%)"].min())
                y_max = max(chart_df["累積 TWR(%)"].max(), chart_df["0050累積(%)"].max())
                y_pad = max(3.0, (y_max - y_min) * 0.18 if y_max != y_min else 3.0)
                fig_perf = go.Figure()
                fig_perf.add_trace(
                    go.Scatter(
                        x=chart_df["日期字串"],
                        y=chart_df["累積 TWR(%)"],
                        mode="lines+markers",
                        name="策略累積 TWR",
                        line=dict(color="#ef4444", width=2.6),
                        marker=dict(size=5),
                    )
                )
                fig_perf.add_trace(
                    go.Scatter(
                        x=chart_df["日期字串"],
                        y=chart_df["0050累積(%)"],
                        mode="lines+markers",
                        name="0050 累積報酬",
                        line=dict(color="#22d3ee", width=2.2, dash="dot"),
                        marker=dict(size=4, color="#22d3ee"),
                    )
                )
                fig_perf.add_hline(y=0, line_dash="dash", line_color="#475569", line_width=1)
                fig_perf.update_layout(
                    template="plotly_dark",
                    height=260,
                    margin=dict(t=20, b=20, l=10, r=10),
                    legend=dict(orientation="h", y=1.02, x=0),
                    hovermode="x unified",
                )
                fig_perf.update_xaxes(type="category", title="日期", tickangle=-35)
                fig_perf.update_yaxes(
                    title="累積績效 (%)",
                    ticksuffix="%",
                    range=[y_min - y_pad, y_max + y_pad],
                )
                st.plotly_chart(fig_perf, use_container_width=True)
            else:
                st.caption("目前有效歷史點數不足，折線圖會在累積更多日期後變得完整。")

            display_df = nav_vis[
                [
                    "日期字串",
                    "NAV",
                    "CashFlow",
                    "日報酬率(%)",
                    "累積指數(=1起)",
                    "累積 TWR(%)",
                    "累積高點",
                    "回撤(%)",
                    "BenchmarkPrice",
                    "BenchmarkDailyReturn",
                    "基準累積指數",
                    "0050累積(%)",
                    "日Alpha(%)",
                    "累積Alpha(%)",
                    "Win?",
                    "操作摘要",
                ]
            ].copy()
            display_df.columns = [
                "日期",
                "投資資產淨值(收盤)",
                "外部現金流(入金+ / 出金-)",
                "日報酬率(TWR)",
                "累積指數(=1起)",
                "累積報酬(你)",
                "累積高點",
                "回撤",
                "基準收盤價(0050)",
                "基準日報酬",
                "基準累積指數",
                "基準累積報酬",
                "日Alpha(你-基準)",
                "累積Alpha",
                "跑贏?(Y/N)",
                "備註",
            ]
            display_df = display_df.sort_values("日期", ascending=False).reset_index(drop=True)

            def perf_text_color(v):
                try:
                    num = float(v)
                    if num > 0:
                        return "color: #ef4444"
                    if num < 0:
                        return "color: #22c55e"
                    return "color: white"
                except Exception:
                    return "color: white"

            def win_text_color(v):
                if v == "Y":
                    return "color: #ef4444; font-weight: 700"
                if v == "N":
                    return "color: #22c55e; font-weight: 700"
                return "color: #cbd5e1"

            format_map = {
                "投資資產淨值(收盤)": "${:,.0f}",
                "外部現金流(入金+ / 出金-)": "${:+,.0f}",
                "日報酬率(TWR)": "{:+.2f}%",
                "累積指數(=1起)": "{:.3f}",
                "累積報酬(你)": "{:+.2f}%",
                "累積高點": "{:.3f}",
                "回撤": "{:.2f}%",
                "基準收盤價(0050)": "${:,.2f}",
                "基準日報酬": "{:+.2f}%",
                "基準累積指數": "{:.3f}",
                "基準累積報酬": "{:+.2f}%",
                "日Alpha(你-基準)": "{:+.2f}%",
                "累積Alpha": "{:+.2f}%",
            }
            with st.expander("查看完整 Daily NAV 明細", expanded=False):
                styled_df = display_df.style.format(format_map)
                styled_df = styled_df.map(
                    perf_text_color,
                    subset=[
                        "日報酬率(TWR)",
                        "累積報酬(你)",
                        "基準日報酬",
                        "基準累積報酬",
                        "日Alpha(你-基準)",
                        "累積Alpha",
                    ],
                )
                styled_df = styled_df.map(win_text_color, subset=["跑贏?(Y/N)"])
                st.dataframe(styled_df, use_container_width=True, height=520)
        else:
            st.info("目前無歷史資料。")

        st.markdown("---")
        st.subheader("每日資產配置比例記錄")
        allocation_name_map = {}
        if not hist_df.empty and "AllocationJson" in hist_df.columns:
            allocation_stock_ids = set()
            for raw_alloc in hist_df["AllocationJson"].fillna("{}").astype(str).tolist():
                try:
                    alloc_obj = json.loads(raw_alloc or "{}")
                except Exception:
                    continue
                allocation_stock_ids.update(
                    normalize_stock_id(asset_key)
                    for asset_key in alloc_obj.keys()
                    if asset_key != "可用現金 (T+0)"
                )
            if allocation_stock_ids:
                allocation_name_map = get_stock_display_names(
                    sorted(allocation_stock_ids)
                )

        alloc_history_df = build_allocation_history_df(hist_df, allocation_name_map)
        if alloc_history_df.empty:
            st.info("目前還沒有可用的每日資產配置快照。")
        else:
            fig_alloc_hist = px.bar(
                alloc_history_df,
                x="日期",
                y="配置比例(%)",
                color="資產",
                title="每日資產配置比例變化",
                text=alloc_history_df["配置比例(%)"].map(
                    lambda v: f"{v:.1f}%" if v >= 5 else ""
                ),
            )
            fig_alloc_hist.update_layout(
                template="plotly_dark",
                barmode="stack",
                height=420,
                margin=dict(t=50, b=20, l=10, r=10),
                yaxis=dict(title="配置比例 (%)", ticksuffix="%"),
                xaxis=dict(title="交易日", type="category"),
                legend=dict(orientation="h", y=1.08, x=0),
            )
            st.plotly_chart(fig_alloc_hist, use_container_width=True)

            alloc_dates = sorted(
                alloc_history_df["日期"].unique().tolist(),
                reverse=True,
            )
            selected_alloc_date = st.selectbox(
                "查看單日配置明細",
                alloc_dates,
                key=f"alloc_analysis_date_{current_pid}",
            )
            day_alloc_df = (
                alloc_history_df[alloc_history_df["日期"] == selected_alloc_date]
                .sort_values("配置比例(%)", ascending=False)
                .reset_index(drop=True)
            )

            d1, d2 = st.columns([1.35, 1])
            with d1:
                fig_alloc_day = px.pie(
                    day_alloc_df,
                    names="資產",
                    values="配置金額",
                    hole=0.45,
                    title=f"{selected_alloc_date} 資產配置",
                    color_discrete_sequence=px.colors.qualitative.Prism,
                )
                fig_alloc_day.update_traces(
                    textposition="inside",
                    textinfo="percent",
                    textfont_size=13,
                )
                fig_alloc_day.update_layout(
                    template="plotly_dark",
                    height=540,
                    margin=dict(t=50, b=20, l=20, r=20),
                    legend=dict(
                        orientation="v",
                        y=0.5,
                        yanchor="middle",
                        x=1.08,
                        font=dict(size=13),
                    ),
                )
                st.plotly_chart(fig_alloc_day, use_container_width=True)
            with d2:
                st.dataframe(
                    day_alloc_df.style.format(
                        {
                            "配置金額": "${:,.0f}",
                            "配置比例(%)": "{:.2f}%",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                    height=540,
                )

    with tab2:
        st.header("交易回顧與操作日誌 (Timeline & PnL)")
        base_review_bundle = get_trade_review_runtime_bundle(current_pid, "", hist_df)
        t_df = base_review_bundle["trades_df"]

        if t_df.empty:
            st.warning("尚無交易紀錄。")
        else:
            t_df["stock_id"] = t_df["stock_id"].apply(normalize_stock_id)
            all_review_stocks = t_df["stock_id"].dropna().unique().tolist()
            display_names = base_review_bundle.get("display_names", {}) or get_stock_display_names(
                all_review_stocks
            )

            t2_tab1, t2_tab2, t2_tab3 = st.tabs(
                ["🎯 單一標的損益追蹤", "📈 已完成交易分析", "📝 所有操作日誌與復盤"]
            )

            with t2_tab1:
                stocks = t_df["stock_id"].unique()
                stock_options = {display_names.get(s, s): s for s in stocks}
                sel_disp_name = st.selectbox("選擇回顧標的", list(stock_options.keys()))
                sel_stock = stock_options[sel_disp_name]

                selected_review_bundle = get_trade_review_runtime_bundle(
                    current_pid, sel_stock, hist_df
                )
                filtered = selected_review_bundle["filtered_df"].copy()
                processed_df = selected_review_bundle["processed_df"].copy()
                trade_cycle_segments = list(
                    selected_review_bundle.get("trade_cycle_segments") or []
                )

                if trade_cycle_segments:
                    cycle_labels = [
                        f"第 {seg['cycle_no']} 輪｜{seg['status']}｜{seg['start_date']} → {seg['end_date']}"
                        for seg in trade_cycle_segments
                    ]
                    selected_cycle_segment_idx = st.selectbox(
                        "選擇交易輪次",
                        range(len(trade_cycle_segments)),
                        format_func=lambda i: cycle_labels[i],
                        key=f"single_stock_cycle_{current_pid}_{sel_stock}",
                    )
                    selected_cycle_segment = trade_cycle_segments[int(selected_cycle_segment_idx)]
                    cycle_processed_df = selected_cycle_segment["rows"].copy()
                else:
                    selected_cycle_segment = None
                    cycle_processed_df = processed_df.copy()

                st.markdown(f"### {sel_disp_name} 交易歷程與動態損益")

                final_pnl = (
                    selected_cycle_segment.get("cycle_realized_pnl", 0.0)
                    if selected_cycle_segment is not None
                    else float(cycle_processed_df.iloc[-1].get("cum_pnl", 0) or 0)
                )
                current_shares = int(cycle_processed_df.iloc[-1]["accum_shares"] or 0)
                current_avg_cost = float(cycle_processed_df.iloc[-1]["avg_cost"] or 0)
                is_closed_cycle = (
                    selected_cycle_segment is not None
                    and selected_cycle_segment["status"] == "已結案"
                )
                if is_closed_cycle:
                    sm1, sm2 = st.columns(2)
                else:
                    sm1, sm2, sm3 = st.columns(3)
                with sm1:
                    render_big_value_card(
                        "本輪累計實現損益",
                        f"${float(final_pnl):+,.1f}",
                        tw_profit_color(float(final_pnl)),
                        card_class="holdings-stat-card",
                    )
                if is_closed_cycle:
                    with sm2:
                        render_big_value_card(
                            "本輪結案平均成本",
                            f"${current_avg_cost:,.2f}",
                            "#38bdf8",
                            card_class="holdings-stat-card",
                        )
                else:
                    with sm2:
                        render_big_value_card(
                            "本輪目前持股",
                            f"{current_shares:,} 股",
                            "#f8fafc",
                            card_class="holdings-stat-card",
                        )
                    with sm3:
                        render_big_value_card(
                            "本輪目前平均成本",
                            f"${current_avg_cost:,.2f}",
                            "#38bdf8",
                            card_class="holdings-stat-card",
                        )

                closed_cycle_df = selected_review_bundle["closed_cycle_df"].copy()
                selected_cycle_no = (
                    int(selected_cycle_segment["cycle_no"])
                    if selected_cycle_segment is not None
                    else None
                )
                selected_cycle_summary = None
                if (
                    is_closed_cycle
                ):
                    st.markdown("#### 已結案交易總結")
                    selected_cycle_summary = render_closed_trade_cycle_summary(
                        closed_cycle_df,
                        key_prefix=f"closed_cycle_{current_pid}_{sel_stock}",
                        selected_cycle_no=selected_cycle_no,
                    )
                    render_ai_trade_cycle_review_panel(
                        current_pid,
                        sel_stock,
                        sel_disp_name,
                        selected_cycle_no,
                        selected_cycle_summary,
                        cycle_processed_df,
                    )
                else:
                    st.info("目前選到的是未結案輪次，等這輪 Close 後就會在這裡產生結案總結與 AI 復盤。")

                # --- NEW CHART & STATS ---
                cycle_start_date = (
                    pd.to_datetime(selected_cycle_segment["start_date"])
                    if selected_cycle_segment is not None
                    else pd.to_datetime(filtered["date"].min())
                )
                cycle_end_date = (
                    pd.to_datetime(selected_cycle_segment["end_date"])
                    if selected_cycle_segment is not None
                    else pd.to_datetime(filtered["date"].max())
                )
                start_date = cycle_start_date - timedelta(days=35)
                # 結束日期設為明天，確保涵蓋最新交易日
                end_date_str = (
                    max(cycle_end_date, pd.to_datetime(datetime.now())) + timedelta(days=1)
                ).strftime("%Y-%m-%d")

                price_hist = fetch_trade_review_price_history(
                    sel_stock,
                    start_date.strftime("%Y-%m-%d"),
                    end_date_str,
                )

                # Check current unrealized PnL for active stock
                avg_cost = current_avg_cost
                if current_shares > 0 and not price_hist.empty:
                    current_price = float(
                        price_hist["Close"].iloc[-1].item()
                        if isinstance(price_hist["Close"].iloc[-1], pd.Series)
                        else price_hist["Close"].iloc[-1]
                    )
                    unrealized_pnl = (current_price - avg_cost) * current_shares
                    unrealized_pct = (
                        (unrealized_pnl / (avg_cost * current_shares) * 100)
                        if avg_cost > 0
                        else 0
                    )
                    st.markdown(
                        f"""
                        <div style="margin:8px 0 14px;color:{tw_profit_color(float(unrealized_pnl))};font-size:16px;font-weight:900;">
                            未實現損益 (Unrealized PnL)：${float(unrealized_pnl):+,.1f} ({float(unrealized_pct):+.2f}%)
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                if not price_hist.empty:
                    fig_s = go.Figure()

                    def safe_col(df, col_name):
                        if col_name in df:
                            d = df[col_name]
                            return d.iloc[:, 0] if isinstance(d, pd.DataFrame) else d
                        return df.iloc[:, 0]

                    close_series = safe_col(price_hist, "Close")

                    fig_s.add_trace(
                        go.Candlestick(
                            x=price_hist.index,
                            open=safe_col(price_hist, "Open"),
                            high=safe_col(price_hist, "High"),
                            low=safe_col(price_hist, "Low"),
                            close=close_series,
                            name="K線走勢",
                            increasing_line_color="#ef4444",
                            decreasing_line_color="#22c55e",
                        )
                    )

                    # Marker traces
                    df_marker = cycle_processed_df.copy()
                    df_marker["date"] = pd.to_datetime(df_marker["date"])

                    buys = df_marker[df_marker["action"].isin(["Buy", "Add", "Setup"])]
                    sells = df_marker[df_marker["action"].isin(["Reduce", "Close"])]

                    if not buys.empty:
                        fig_s.add_trace(
                            go.Scatter(
                                x=buys["date"],
                                y=buys["price"],
                                mode="markers",
                                name="建立/加碼庫存",
                                marker=dict(
                                    color="#22c55e",
                                    symbol="triangle-up",
                                    size=14,
                                    line=dict(width=1, color="white"),
                                ),
                            )
                        )
                    if not sells.empty:
                        fig_s.add_trace(
                            go.Scatter(
                                x=sells["date"],
                                y=sells["price"],
                                mode="markers",
                                name="減碼/平倉",
                                marker=dict(
                                    color="#ef4444",
                                    symbol="triangle-down",
                                    size=14,
                                    line=dict(width=1, color="white"),
                                ),
                            )
                        )

                    fig_s.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
                    fig_s.update_layout(
                        template="plotly_dark",
                        margin=dict(t=20, b=20, l=10, r=10),
                        height=350,
                        xaxis_rangeslider_visible=False,
                    )
                    st.plotly_chart(fig_s, use_container_width=True)
                else:
                    st.warning("無法載入歷史股價走勢，可能是代號格式問題。")

                st.markdown("##### 詳細操作歷程")
                for idx, row in cycle_processed_df.iterrows():
                    action_emoji = (
                        "🟢"
                        if row["action"] in ["Buy", "Add", "Setup"]
                        else ("🔴" if row["action"] == "Close" else "🟠")
                    )
                    action_name = {
                        "Buy": "首筆買進",
                        "Add": "加碼",
                        "Setup": "期初建倉",
                        "Reduce": "減碼/部分停利",
                        "Close": "全數平倉",
                    }.get(row["action"], row["action"])

                    with st.expander(
                        f"{action_emoji} {row['date']}  -  {action_name}  ({row['shares']} 股, @${row['price']:.1f})",
                        expanded=True,
                    ):
                        cols = st.columns(3)
                        cols[0].metric("成交單價", f"${row['price']:.1f}")
                        cols[1].metric("帳面加權均價", f"${row['avg_cost']:.1f}")

                        if row["action"] in ["Reduce", "Close"]:
                            pnl = row["realized_pnl"]
                            pnl_pct = (
                                (row["price"] - row["avg_cost"]) / row["avg_cost"] * 100
                                if row["avg_cost"] > 0
                                else 0
                            )
                            pnl_color = tw_profit_color(float(pnl))
                            cols[2].markdown(
                                f"""
                                <div style="padding-top:2px;">
                                  <div style="color:#94a3b8;font-size:14px;font-weight:700;">此筆實現損益</div>
                                  <div style="color:{pnl_color};font-size:28px;font-weight:900;line-height:1.2;">
                                    ${float(pnl):+,.1f}
                                  </div>
                                  <div style="color:{pnl_color};font-size:14px;font-weight:800;">
                                    {float(pnl_pct):+.2f}%
                                  </div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )

                        render_trade_risk_target_summary(row)
                        render_trade_score_summary(row, f"single_stock_{idx}")
                        render_structured_trade_notes(row.get("trading_notes", ""))

            with t2_tab2:
                render_closed_trade_analytics_panel(t_df, display_names)

            with t2_tab3:
                st.markdown("### 歷史所有交易流水帳")
                st.caption(
                    "依日期由最早到最近排列，點開每筆交易可閱讀完整心得、三面向評分，並直接修改原始紀錄。"
                )
                view_log_df = t_df.copy()
                view_log_df["display_stock_name"] = view_log_df["stock_id"].apply(
                    lambda x: display_names.get(x, x)
                )
                view_log_df["display_action_name"] = view_log_df["action"].apply(
                    lambda x: {
                        "Buy": "首筆買進",
                        "Add": "加碼",
                        "Setup": "期初建倉",
                        "Reduce": "減碼/停利",
                        "Close": "全數平倉",
                        "Dividend": "除息分配",
                    }.get(x, x)
                )
                view_log_df.sort_values(
                    ["date", "id"], ascending=[True, True], inplace=True
                )
                selected_edit_trade_row = None

                for idx, row in view_log_df.iterrows():
                    with st.expander(
                        f"{row['date']}｜{row['display_stock_name']}｜{row['display_action_name']}｜{int(row['shares'])} 股 @ ${float(row['price']):.2f}",
                        expanded=False,
                    ):
                        detail_cols = st.columns(5)
                        detail_cols[0].metric("成交價", f"${float(row['price']):.2f}")
                        detail_cols[1].metric("股數", f"{int(row['shares']):,}")
                        detail_cols[2].metric("交易動作", row["display_action_name"])
                        detail_cols[3].metric(
                            "日期", pd.to_datetime(row["date"]).strftime("%Y-%m-%d")
                        )
                        if detail_cols[4].button(
                            "✏️ 編輯 / 刪除",
                            key=f"open_trade_edit_{current_pid}_{int(row['id'])}",
                            use_container_width=True,
                        ):
                            st.session_state["show_trade_entry_dialog"] = False
                            st.session_state["editing_trade_id"] = int(row["id"])
                            st.rerun()

                        render_trade_score_summary(row, f"all_trade_log_{int(row['id'])}")
                        render_trade_risk_target_summary(row)
                        render_structured_trade_notes(row.get("trading_notes", ""))

                    if st.session_state.get("editing_trade_id") == int(row["id"]):
                        selected_edit_trade_row = row

                if selected_edit_trade_row is not None:
                    render_trade_edit_dialog(selected_edit_trade_row, current_pid)

    with tab4:
        st.header("外部資金管理")
        with st.form("cash_flow_form"):
            cf_date = st.date_input("入帳日期", datetime.now())
            st.info(
                "💡 系統已全面自動化除權息追蹤！您不需要手動輸入配息（入息日當天系統會自動結算並加進 T+0 現金池中），請僅在**實體銀行帳戶間轉帳**時輸入出入金。"
            )
            cf_type_display = st.selectbox(
                "類型", ["Deposit (入金)", "Withdrawal (出金)"]
            )
            cf_type = "Deposit" if "Deposit" in cf_type_display else "Withdrawal"
            cf_amount = st.number_input("外部轉帳金額", value=0.0, step=10000.0)
            if st.form_submit_button("寫入資金流"):
                execute_cashflow(
                    current_pid, cf_date.strftime("%Y-%m-%d"), cf_type, cf_amount
                )
                invalidate_portfolio_runtime_bundle(current_pid)
                show_ui_toast(f"已寫入資金流：{cf_type} ${cf_amount:,.0f}")
                st.rerun()
