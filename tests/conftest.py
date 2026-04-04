from datetime import datetime

import pandas as pd
import pytest

import backend


@pytest.fixture
def stable_market_data(monkeypatch):
    monkeypatch.setattr(backend, "sync_twse_market_holidays", lambda target_years=None: 0)
    monkeypatch.setattr(
        backend,
        "get_stock_display_names",
        lambda tickers: {t: f"{backend.normalize_stock_id(t)} 測試股" for t in tickers},
    )
    monkeypatch.setattr(
        backend,
        "fetch_finmind_last_close",
        lambda stock_id, lookback_days=45: {"2330": 1810.0, "0050": 73.95}.get(
            backend.normalize_stock_id(stock_id), 100.0
        ),
    )

    def fake_fetch_yfinance_history(tickers, start_date=None, period="1mo"):
        idx = pd.to_datetime(
            ["2026-03-27", "2026-03-30", "2026-03-31", "2026-04-01", "2026-04-02"]
        )
        close_df = pd.DataFrame(index=idx)
        divs_df = pd.DataFrame(index=idx)
        symbol_map = {}
        for ticker in tickers:
            sid = backend.normalize_stock_id(ticker)
            symbol_map[sid] = f"{sid}.TW"
            if sid == "2330":
                close_df[sid] = [780.0, 1780.0, 1760.0, 1855.0, 1810.0]
            elif sid == "0050":
                close_df["0050.TW"] = [75.0, 73.9, 72.35, 75.45, 73.95]
            else:
                close_df[sid] = [100.0, 101.0, 102.0, 103.0, 104.0]
            divs_df[sid] = 0.0
        if "0050.TW" not in close_df.columns:
            close_df["0050.TW"] = [75.0, 73.9, 72.35, 75.45, 73.95]
        divs_df["0050.TW"] = 0.0
        symbol_map.setdefault("0050", "0050.TW")
        return close_df, divs_df, symbol_map

    monkeypatch.setattr(backend, "fetch_yfinance_history", fake_fetch_yfinance_history)
    return True


@pytest.fixture
def temp_portfolio(stable_market_data):
    backend.ensure_db_schema()
    portfolio_name = f"__pytest_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    portfolio_id = backend.create_portfolio(portfolio_name, initial_cash=300000)
    try:
        yield portfolio_id
    finally:
        backend.delete_portfolio_and_related_data(portfolio_id)
