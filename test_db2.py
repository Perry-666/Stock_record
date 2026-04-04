import traceback
from backend import get_holdings_detail, get_portfolios

ports = get_portfolios()
if not ports.empty:
    for idx, r in ports.iterrows():
        pid = r['id']
        try:
            get_holdings_detail(pid)
        except Exception as e:
            print(f"Error in ID {pid}:")
            traceback.print_exc()
