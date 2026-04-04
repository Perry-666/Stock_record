with open('app.py', 'r') as f:
    text = f.read()

text = text.replace(
'''        with st.spinner("載入報價與 TWR 計算中..."):
        hist_df, _, current_twr = calculate_twr_and_nav(current_pid)
        holdings_df, raw_holdings = get_holdings_detail(current_pid)''',
'''        with st.spinner("載入報價與 TWR 計算中..."):
            hist_df, _, current_twr = calculate_twr_and_nav(current_pid)
            holdings_df, raw_holdings = get_holdings_detail(current_pid)'''
)

text = text.replace(
'''        with col1:
        st.subheader("目前資產配置")''',
'''        with col1:
            st.subheader("目前資產配置")'''
)

text = text.replace(
'''        if not holdings_df.empty:
        def color_pnl(val):''',
'''        if not holdings_df.empty:
            def color_pnl(val):'''
)

text = text.replace(
'''        else:
        st.info("目前無持股，資金均閒置於現金庫備戰。")''',
'''        else:
            st.info("目前無持股，資金均閒置於現金庫備戰。")'''
)

text = text.replace(
'''        if not hist_df.empty:
        display_df = hist_df[['Date', 'Notes', 'NAV', 'CashFlow', 'DailyReturn', 'TWR', 'Drawdown', 'BenchmarkPrice', 'BenchmarkDailyReturn', 'BenchmarkCumReturn', 'DailyAlpha', 'CumAlpha', 'Win?']].copy()''',
'''        if not hist_df.empty:
            display_df = hist_df[['Date', 'Notes', 'NAV', 'CashFlow', 'DailyReturn', 'TWR', 'Drawdown', 'BenchmarkPrice', 'BenchmarkDailyReturn', 'BenchmarkCumReturn', 'DailyAlpha', 'CumAlpha', 'Win?']].copy()'''
)

text = text.replace(
'''        else:
        st.info("目前無歷史資料。")''',
'''        else:
            st.info("目前無歷史資料。")'''
)

text = text.replace(
'''with tab2:
        st.header("交易回顧與操作日誌 (Timeline & PnL)")''',
'''    with tab2:
        st.header("交易回顧與操作日誌 (Timeline & PnL)")'''
)

with open('app.py', 'w') as f:
    f.write(text)

