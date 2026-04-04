import yfinance as yf

data = yf.download("2330.TW", period="6mo", actions=True, progress=False)
if data.empty:
    print("No price/dividend data returned.")
else:
    top_level_cols = data.columns.get_level_values(0).unique()
    print(top_level_cols)
    if "Dividends" not in top_level_cols:
        print("No Dividends column returned in this response.")
    else:
        divs = data["Dividends"]
        print(divs[divs > 0].dropna(how="all"))
