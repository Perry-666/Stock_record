import json

import pandas as pd
import pytest

import backend


def test_latest_official_trading_date_before_and_after_close(monkeypatch):
    monkeypatch.setattr(
        backend,
        "get_market_holiday_dates",
        lambda: {"2026-04-03", "2026-04-06"},
    )

    monkeypatch.setattr(
        backend,
        "get_tw_now",
        lambda: pd.Timestamp("2026-04-08 11:00:00").to_pydatetime(),
    )
    assert backend.get_latest_official_tw_trading_date() == "2026-04-07"

    monkeypatch.setattr(
        backend,
        "get_tw_now",
        lambda: pd.Timestamp("2026-04-08 14:10:00").to_pydatetime(),
    )
    assert backend.get_latest_official_tw_trading_date() == "2026-04-08"


def test_trade_edit_delete_rebuilds_nav_and_blocks_invalid_actions(temp_portfolio):
    backend.execute_cashflow(temp_portfolio, "2026-03-27", "Deposit", 20000)
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "init note",
        technical_score=4,
        chip_score=5,
        theme_score=4,
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-03-31",
        "2330",
        "Add",
        790,
        100,
        False,
        True,
        True,
        True,
        True,
        "",
        technical_score=3,
        chip_score=4,
        theme_score=3,
    )

    with pytest.raises(ValueError, match="休市日"):
        backend.execute_trade(
            temp_portfolio,
            "2026-04-03",
            "2330",
            "Add",
            800,
            100,
            False,
            True,
            True,
            True,
            True,
            "",
        )

    with pytest.raises(ValueError, match="Buy 首次買進"):
        backend.execute_trade(
            temp_portfolio,
            "2026-04-01",
            "2330",
            "Buy",
            800,
            100,
            False,
            True,
            True,
            True,
            True,
            "",
        )

    trades_df = backend.get_portfolio_trades_df(temp_portfolio)
    buy_trade_id = int(trades_df.iloc[0]["id"])
    add_trade_id = int(trades_df.iloc[1]["id"])

    backend.update_trade_record(
        temp_portfolio,
        buy_trade_id,
        "2026-03-30",
        "2330",
        "Buy",
        781,
        100,
        False,
        "edited notes",
        technical_score=5,
        chip_score=4,
        theme_score=3,
    )

    with pytest.raises(ValueError, match="不能使用 Add"):
        backend.delete_trade_record(temp_portfolio, buy_trade_id)
    backend.delete_trade_record(temp_portfolio, add_trade_id)

    holdings_df, lot_rows = backend.get_holdings_detail(temp_portfolio)
    hist_df, current_nav, current_twr = backend.calculate_twr_and_nav(temp_portfolio)
    final_trades_df = backend.get_portfolio_trades_df(temp_portfolio)
    daily_snapshot_df = backend.get_daily_nav_snapshots_df(temp_portfolio)

    assert len(final_trades_df) == 1
    assert int(final_trades_df.iloc[0]["technical_score"]) == 5
    assert final_trades_df.iloc[0]["trading_notes"] == "edited notes"
    assert int(holdings_df.iloc[0]["持有股數"]) == 100
    assert round(float(hist_df.iloc[0]["NAV"]), 2) == 320000.00
    assert len(daily_snapshot_df) == len(hist_df)
    assert backend.portfolio_repository.get_nav_dirty_from_date(temp_portfolio) is None
    assert float(current_nav) == float(hist_df.iloc[-1]["NAV"])
    assert float(current_twr) == float(hist_df.iloc[-1]["TWR"])
    assert lot_rows and lot_rows[0]["Stock"] == "2330"


def test_settlement_date_skips_holidays_and_weekends():
    old_holidays_df = backend.get_market_holidays_df()
    old_apr7 = old_holidays_df[old_holidays_df["date"] == "2026-04-07"].copy()

    backend.upsert_market_holiday("2026-04-03", "兒童節補假", False)
    backend.upsert_market_holiday("2026-04-06", "清明補假", False)
    backend.upsert_market_holiday("2026-04-07", "休市但可交割測試", True)

    try:
        assert backend.estimate_settlement_date("2026-04-01") == "2026-04-07"
        blocked_dates = backend.get_settlement_blocked_dates()
        assert "2026-04-03" in blocked_dates
        assert "2026-04-06" in blocked_dates
        assert "2026-04-07" not in blocked_dates
    finally:
        if old_apr7.empty:
            backend.delete_market_holiday("2026-04-07")
        else:
            row = old_apr7.iloc[0]
            backend.upsert_market_holiday(
                row["date"],
                row.get("reason", ""),
                bool(row.get("is_settlement_open", False)),
            )


def test_manual_override_has_highest_priority(temp_portfolio):
    backend.execute_cashflow(temp_portfolio, "2026-03-27", "Deposit", 200000)
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "override test",
    )

    backend.set_manual_price_override("2330", 2000)
    try:
        holdings_df, _ = backend.get_holdings_detail(temp_portfolio)
    finally:
        backend.delete_manual_price_override("2330")

    assert not holdings_df.empty
    assert float(holdings_df.iloc[0]["當前現價"]) == 2000.0
    assert holdings_df.iloc[0]["報價來源"] == "手動覆蓋"


def test_price_snapshot_roundtrip_and_nav_snapshot_cache(temp_portfolio):
    backend.execute_cashflow(temp_portfolio, "2026-03-27", "Deposit", 10000)
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "snapshot test",
    )

    price_df = pd.DataFrame(
        {"2330": [1780.0, 1760.0], "0050": [73.9, 72.35]},
        index=pd.to_datetime(["2026-03-30", "2026-03-31"]),
    )
    written = backend.upsert_price_snapshots(price_df, source="pytest")
    assert written == 4

    snapshot_df = backend.get_price_snapshot_history(
        ["2330", "0050"], start_date="2026-03-30", end_date="2026-03-31"
    )
    assert float(snapshot_df.loc[pd.Timestamp("2026-03-31"), "2330"]) == 1760.0
    assert backend.get_price_snapshot_min_latest_date(["2330", "0050"]) >= "2026-03-31"

    hist_df, _, _ = backend.calculate_twr_and_nav(temp_portfolio)
    cached_df = backend.get_daily_nav_snapshots_df(temp_portfolio)
    assert len(hist_df) == len(cached_df)
    assert not cached_df.empty


def test_calculate_twr_and_nav_ignores_future_dated_events_without_crashing(
    monkeypatch, temp_portfolio
):
    backend.execute_cashflow(temp_portfolio, "2026-03-27", "Deposit", 10000)
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "base position",
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-04-16",
        "2330",
        "Add",
        790,
        100,
        False,
        True,
        True,
        True,
        True,
        "future add",
    )

    monkeypatch.setattr(
        backend,
        "get_tw_now",
        lambda: pd.Timestamp("2026-04-15 14:30:00").to_pydatetime(),
    )

    hist_df, current_nav, current_twr = backend.calculate_twr_and_nav(temp_portfolio)

    assert not hist_df.empty
    assert hist_df.iloc[-1]["Date"] == "2026-04-15"
    assert float(hist_df.iloc[-1]["Holdings"]) == 181000.0
    assert float(current_nav) == pytest.approx(float(hist_df.iloc[-1]["NAV"]))
    assert float(current_twr) == pytest.approx(float(hist_df.iloc[-1]["TWR"]))


def test_process_trade_derivation_uses_side_and_historical_inventory(temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "first buy",
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-03-31",
        "2330",
        "Add",
        790,
        100,
        False,
        True,
        True,
        True,
        True,
        "add position",
    )

    assert (
        backend.process_trade_derivation(
            temp_portfolio,
            {
                "trade_date": "2026-03-29",
                "stock_id": "2330",
                "side": "buy",
                "shares": 100,
            },
        )
        == "Buy"
    )
    assert (
        backend.process_trade_derivation(
            temp_portfolio,
            {
                "trade_date": "2026-04-01",
                "stock_id": "2330",
                "side": "buy",
                "shares": 100,
            },
        )
        == "Add"
    )
    assert (
        backend.process_trade_derivation(
            temp_portfolio,
            {
                "trade_date": "2026-04-01",
                "stock_id": "2330",
                "side": "sell",
                "shares": 100,
            },
        )
        == "Reduce"
    )
    assert (
        backend.process_trade_derivation(
            temp_portfolio,
            {
                "trade_date": "2026-04-01",
                "stock_id": "2330",
                "side": "sell",
                "shares": 200,
            },
        )
        == "Close"
    )


def test_holdings_detail_has_holding_days_efficiency_and_risk_targets(temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "risk plan test",
        take_profit_price=2000,
        stop_loss_price=1700,
    )

    holdings_df, _ = backend.get_holdings_detail(temp_portfolio)
    assert not holdings_df.empty
    row = holdings_df.iloc[0]
    assert int(row["持有天數"]) >= 4
    assert float(row["報酬效率 (%/日)"]) > 0
    assert float(row["停利目標價"]) == 2000
    assert float(row["停損目標價"]) == 1700
    assert float(row["距停利 (%)"]) > 0
    assert float(row["距停損 (%)"]) < 0

    backend.update_holding_risk_targets(
        temp_portfolio,
        "2330",
        take_profit_price=1950,
        stop_loss_price=1650,
    )
    updated_holdings_df, _ = backend.get_holdings_detail(temp_portfolio)
    updated_row = updated_holdings_df.iloc[0]
    assert float(updated_row["停利目標價"]) == 1950
    assert float(updated_row["停損目標價"]) == 1650


def test_add_reduce_new_risk_targets_override_current_holding_but_keep_old_trade_records(temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "initial plan",
        take_profit_price=900,
        stop_loss_price=740,
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-03-31",
        "2330",
        "Add",
        790,
        100,
        False,
        True,
        True,
        True,
        True,
        "new plan after add",
        take_profit_price=950,
        stop_loss_price=760,
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-04-01",
        "2330",
        "Reduce",
        800,
        100,
        False,
        True,
        True,
        True,
        True,
        "raise stop after reduce",
        take_profit_price=980,
        stop_loss_price=770,
    )

    holdings_df, _ = backend.get_holdings_detail(temp_portfolio)
    assert not holdings_df.empty
    holding_row = holdings_df.iloc[0]
    assert float(holding_row["停利目標價"]) == 980
    assert float(holding_row["停損目標價"]) == 770

    trades_df = backend.get_portfolio_trades_df(temp_portfolio).sort_values(["date", "id"])
    buy_row = trades_df[trades_df["action"] == "Buy"].iloc[0]
    add_row = trades_df[trades_df["action"] == "Add"].iloc[0]
    reduce_row = trades_df[trades_df["action"] == "Reduce"].iloc[0]
    assert float(buy_row["take_profit_price"]) == 900
    assert float(buy_row["stop_loss_price"]) == 740
    assert float(add_row["take_profit_price"]) == 950
    assert float(add_row["stop_loss_price"]) == 760
    assert float(reduce_row["take_profit_price"]) == 980
    assert float(reduce_row["stop_loss_price"]) == 770


def test_stale_allocation_snapshot_with_deleted_asset_is_rebuilt(temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "allocation test",
    )

    hist_df, _, _ = backend.calculate_twr_and_nav(temp_portfolio)
    stale_df = hist_df.copy()
    stale_payload = json.loads(stale_df.iloc[0]["AllocationJson"])
    stale_payload["9999"] = {"value": 1234.0, "weight_pct": 0.4}
    stale_df.loc[stale_df.index[0], "AllocationJson"] = json.dumps(
        stale_payload,
        ensure_ascii=False,
    )
    backend.upsert_daily_nav_snapshots(temp_portfolio, stale_df)
    backend.portfolio_repository.set_nav_dirty_from_date(temp_portfolio, None)

    refreshed_df, _, _ = backend.calculate_twr_and_nav(temp_portfolio)
    assert not refreshed_df.empty
    assert all(
        "9999" not in json.loads(snapshot_json or "{}")
        for snapshot_json in refreshed_df["AllocationJson"].tolist()
    )


def test_closed_trade_cycle_summary_and_close_cost_basis(temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "cycle open",
        technical_score=4,
        chip_score=4,
        theme_score=4,
        take_profit_price=1900,
        stop_loss_price=700,
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-03-31",
        "2330",
        "Add",
        790,
        100,
        False,
        True,
        True,
        True,
        True,
        "cycle add",
        technical_score=4,
        chip_score=3,
        theme_score=4,
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-04-01",
        "2330",
        "Reduce",
        1800,
        100,
        False,
        True,
        True,
        True,
        True,
        "cycle reduce",
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-04-02",
        "2330",
        "Close",
        1810,
        100,
        False,
        True,
        True,
        True,
        True,
        "cycle close",
    )

    trades_df = backend.get_portfolio_trades_df(temp_portfolio)
    processed_df = backend.calculate_trade_journal(trades_df[trades_df["stock_id"] == "2330"])
    close_row = processed_df[processed_df["action"] == "Close"].iloc[-1]
    assert round(float(close_row["avg_cost"]), 2) == 785.00
    assert round(float(close_row["realized_pnl"]), 2) == 102500.00

    hist_df, _, _ = backend.calculate_twr_and_nav(temp_portfolio)
    cycle_df = backend.summarize_closed_stock_trade_cycles(
        temp_portfolio,
        "2330",
        portfolio_hist_df=hist_df,
    )
    assert len(cycle_df) == 1
    summary_row = cycle_df.iloc[0]
    assert round(float(summary_row["結算損益"]), 2) == 204000.00
    assert float(summary_row["結算報酬率 (%)"]) > 100
    assert float(summary_row["最大投入成本"]) >= 156000
    assert float(summary_row["最大浮盈"]) > 0
    assert float(summary_row["最大浮虧"]) <= 0
    assert "結案獲利" in str(summary_row["操作總和評估"])


def test_closed_trade_cycle_summary_separates_multiple_round_trips(temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "first cycle buy",
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-03-31",
        "2330",
        "Close",
        800,
        100,
        True,
        True,
        True,
        True,
        True,
        "first cycle close",
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-04-01",
        "2330",
        "Buy",
        790,
        100,
        False,
        True,
        True,
        True,
        True,
        "second cycle buy",
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-04-02",
        "2330",
        "Close",
        830,
        100,
        True,
        True,
        True,
        True,
        True,
        "second cycle close",
    )

    hist_df, _, _ = backend.calculate_twr_and_nav(temp_portfolio)
    cycle_df = backend.summarize_closed_stock_trade_cycles(
        temp_portfolio,
        "2330",
        portfolio_hist_df=hist_df,
    ).sort_values("輪次")

    assert cycle_df["輪次"].tolist() == [1, 2]
    first_cycle = cycle_df.iloc[0]
    second_cycle = cycle_df.iloc[1]
    assert first_cycle["進場日"] == "2026-03-30"
    assert first_cycle["出場日"] == "2026-03-31"
    assert round(float(first_cycle["結算損益"]), 2) == 2000.00
    assert second_cycle["進場日"] == "2026-04-01"
    assert second_cycle["出場日"] == "2026-04-02"
    assert round(float(second_cycle["結算損益"]), 2) == 4000.00


def test_ai_review_closed_trade_cycle_serializes_numpy_values(monkeypatch, temp_portfolio):
    backend.execute_trade(
        temp_portfolio,
        "2026-03-30",
        "2330",
        "Buy",
        780,
        100,
        False,
        True,
        True,
        True,
        True,
        "AI review buy",
        technical_score=4,
        chip_score=4,
        theme_score=5,
        take_profit_price=900,
        stop_loss_price=740,
    )
    backend.execute_trade(
        temp_portfolio,
        "2026-03-31",
        "2330",
        "Close",
        800,
        100,
        True,
        True,
        True,
        True,
        True,
        "AI review close",
        technical_score=3,
        chip_score=3,
        theme_score=3,
    )

    hist_df, _, _ = backend.calculate_twr_and_nav(temp_portfolio)
    trades_df = backend.get_portfolio_trades_df(temp_portfolio)
    processed_df = backend.calculate_trade_journal(trades_df[trades_df["stock_id"] == "2330"])
    cycle_df = backend.summarize_closed_stock_trade_cycles(
        temp_portfolio,
        "2330",
        portfolio_hist_df=hist_df,
    )

    captured_prompt = {}

    def _mock_call(prompt):
        captured_prompt["text"] = prompt
        return "MOCK AI REVIEW", None

    monkeypatch.setattr(backend, "_call_gemini_text_with_fallback", _mock_call)
    review_text = backend.ai_review_closed_trade_cycle(
        "2330 台積電",
        cycle_df.iloc[0],
        processed_df,
    )

    assert review_text == "MOCK AI REVIEW"
    assert "本輪結案統計 JSON" in captured_prompt["text"]
    assert "本輪逐筆交易與心得 JSON" in captured_prompt["text"]
    assert "AI review buy" in captured_prompt["text"]
