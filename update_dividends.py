import re

with open("app.py", "r") as f:
    code = f.read()

# 1. Update get_historical_prices to return divs_data
hist_prices_old = """    try:
        data = yf.download(yf_tickers, start=start_date, auto_adjust=True)
        if isinstance(data.columns, pd.MultiIndex):
            close_data = data["Close"]
        else:
            close_data = pd.DataFrame(data["Close"]).rename(
                columns={"Close": yf_tickers[0]}
            )

        close_data = close_data.ffill()
        return close_data, symbol_map
    except Exception as e:
        print(f"yfinance error: {e}")
        return pd.DataFrame(), symbol_map"""

hist_prices_new = """    try:
        data = yf.download(yf_tickers, start=start_date, auto_adjust=False, actions=True, progress=False)
        if isinstance(data.columns, pd.MultiIndex):
            close_data = data["Close"] if "Close" in data else pd.DataFrame(columns=yf_tickers)
            divs_data = data["Dividends"] if "Dividends" in data else pd.DataFrame(columns=yf_tickers)
        else:
            close_data = pd.DataFrame(data["Close"]).rename(
                columns={"Close": yf_tickers[0]}
            )
            divs_data = pd.DataFrame(data["Dividends"] if "Dividends" in data else pd.Series(0, index=data.index)).rename(
                columns={"Dividends": yf_tickers[0]}
            )

        close_data = close_data.ffill()
        return close_data, divs_data, symbol_map
    except Exception as e:
        print(f"yfinance error: {e}")
        return pd.DataFrame(), pd.DataFrame(), symbol_map"""

code = code.replace(hist_prices_old, hist_prices_new)

# 2. Add get_stock_dividends & update calculate_trade_journal
trade_j_old = """def calculate_trade_journal(trades_df):
    records = []
    avg_cost = 0.0
    accum_shares = 0
    cum_pnl = 0.0

    for idx, row in trades_df.iterrows():
        action = row["action"]
        shares = row["shares"]
        price = row["price"]

        pnl_this_trade = 0.0

        if action in ["Buy", "Add", "Setup"]:
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

    return pd.DataFrame(records)"""

trade_j_new = """@st.cache_data(ttl=86400)
def get_stock_dividends(stock_id, start_date):
    yf_code = stock_id + ".TW" if not (stock_id.endswith(".TW") or stock_id.endswith(".TWO")) else stock_id
    try:
        data = yf.download(yf_code, start=start_date, auto_adjust=False, actions=True, progress=False)
        if "Dividends" in data:
            d = data["Dividends"]
            if isinstance(d, pd.DataFrame): d = d.iloc[:, 0]
            d = d[d > 0].dropna()
            return d
    except: pass
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
                div_rows.append({
                    "id": -1, "portfolio_id": trades_df.iloc[0]["portfolio_id"], 
                    "date": d.strftime("%Y-%m-%d"), "stock_id": s_id, 
                    "action": "Dividend", "price": float(amt), "shares": 0,
                    "is_disposed": 0, "disposed_cost": 0, "disposed_tax": 0,
                    "trading_notes": f"系統自動除權息: ${float(amt):.2f}/股"
                })
        if div_rows:
            trades_df = pd.concat([trades_df, pd.DataFrame(div_rows)]).sort_values("date").reset_index(drop=True)

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

    return pd.DataFrame(records)"""

code = code.replace(trade_j_old, trade_j_new)

# 3. Update calculate_twr_and_nav to use divs_data
code = code.replace("price_data, symbol_map = get_historical_prices(symbols, start_date_str)", "price_data, divs_data, symbol_map = get_historical_prices(symbols, start_date_str)")

nav_loop_old_part1 = """        inv_value = 0.0
        for s_id, sh in current_inv.items():
            if sh > 0:
                yf_code = symbol_map.get(s_id)
                if not price_data.empty and yf_code in price_data.columns:
                    sub_data = price_data.loc[:d]"""

nav_loop_new_part1 = """        inv_value = 0.0
        for s_id, sh in current_inv.items():
            if sh > 0:
                yf_code = symbol_map.get(s_id)
                
                if not divs_data.empty and yf_code in divs_data.columns:
                    if d in divs_data.index.strftime("%Y-%m-%d"):
                        div_v = divs_data[yf_code].loc[d]
                        if isinstance(div_v, pd.Series): div_v = div_v.iloc[0]
                        if pd.notna(div_v) and div_v > 0:
                            # 股票除息！將配息直接納入現金！(外部現金流不變，因為這是資本池內部轉換)
                            current_cash += float(div_v) * sh
                            
                if not price_data.empty and yf_code in price_data.columns:
                    sub_data = price_data.loc[:d]"""

code = code.replace(nav_loop_old_part1, nav_loop_new_part1)

# 4. Replace single stock tracking timedelta 21 -> 14-30 as 30, and make Candlestick robust Y-axis, auto_adjust=False
code = code.replace("start_date = pd.to_datetime(filtered[\"date\"].min()) - timedelta(days=21)", "start_date = pd.to_datetime(filtered[\"date\"].min()) - timedelta(days=35)")
code = code.replace("price_hist = yf.download(", "price_hist = yf.download(\n                    actions=True,\n                    auto_adjust=False,")

# Candlestick y-buffer logic if needed (wait Plotly does auto range, let's keep it clean without forcing unless we need to)

# Also fix the "st.markdown('##### 詳細操作歷程')" since we have Dividend actions now:
code = code.replace('{"Buy": "首筆買進", "Add": "加碼", "Setup": "期初建倉", "Reduce": "減碼/部分停利", "Close": "全數平倉"}.get(row[\'action\'], row[\'action\'])', '{"Buy": "首筆買進", "Add": "加碼", "Setup": "期初建倉", "Reduce": "減碼/部分停利", "Close": "全數平倉", "Dividend": "除息分配"}.get(row[\'action\'], row[\'action\'])')
code = code.replace('"🟢" if row["action"] in ["Buy", "Add", "Setup"] else ("🔴" if row["action"] == "Close" else "🟠")', '"🟢" if row["action"] in ["Buy", "Add", "Setup"] else ("💰" if row["action"] == "Dividend" else ("🔴" if row["action"] == "Close" else "🟠"))')

with open("app.py", "w") as f:
    f.write(code)

print("Replacement successful")
