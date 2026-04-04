import sqlite3
import pandas as pd
from backend import (
    calculate_twr_and_nav,
    get_holdings_detail,
    get_portfolios,
    get_stock_display_names,
)

ports = get_portfolios()
if not ports.empty:
    for idx, r in ports.iterrows():
        pid = r['id']
        print(f"Testing Portfolio: {r['name']} (ID: {pid})")
        
        try:
            hist_df, _, twr = calculate_twr_and_nav(pid)
            print("calculate_twr_and_nav Success.")
        except Exception as e:
            import traceback
            print("calculate_twr_and_nav ERROR:")
            traceback.print_exc()

        try:
            holdings_df, active_list = get_holdings_detail(pid)
            print("get_holdings_detail Success.")
        except Exception as e:
            import traceback
            print("get_holdings_detail ERROR:")
            traceback.print_exc()
