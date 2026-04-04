import re

with open("app.py", "r") as f:
    code = f.read()

# 1. Remove the AI block from sidebar
ai_block_pattern = r'        st\.markdown\("---"\)\n        st\.header\("🤖 AI 交易辨識與入帳"\).*?st\.rerun\(\)\n'
ai_block_match = re.search(ai_block_pattern, code, re.DOTALL)
if ai_block_match:
    ai_block_code_raw = ai_block_match.group(0)
    ai_block_code_dedented = "\n".join([line[4:] if line.startswith("    ") else line for line in ai_block_code_raw.split("\n")[:-1]]) + "\n"
    code = code[:ai_block_match.start()] + code[ai_block_match.end():]
else:
    print("Could not find AI block!")
    exit(1)

# 2. Add Tab Trade
tabs_pattern = r'    tab1, tab2, tab4 = st\.tabs\(\["📊 總合儀表板", "📝 交易回顧中心", "💰 資金出入"\]\)'
if tabs_pattern not in code and 'tab1, tab_trade' not in code:
    code = code.replace(
        'tab1, tab2, tab4 = st.tabs(["📊 總合儀表板", "📝 交易回顧中心", "💰 資金出入"])',
        'tab1, tab_trade, tab2, tab4 = st.tabs(["📊 總合儀表板", "➕ AI 交易入帳", "📝 交易回顧中心", "💰 資金出入"])'
    )

# 3. Insert Trade block right before `with tab2:`
tab2_idx = code.find('    with tab2:')
if tab2_idx != -1:
    trade_tab_code = f"""    with tab_trade:
{ai_block_code_dedented}
"""
    code = code[:tab2_idx] + trade_tab_code + code[tab2_idx:]
else:
    print("Could not find tab2!")
    exit(1)

# 4. Enhance single stock UI
old_pnl = r'''                st\.metric\("累計實現損益 \(Realized PnL\)", f"\$\{final_pnl:,\.1f\}"\)

                for idx, row in processed_df\.iterrows\(\):'''

new_pnl = '''                st.metric("累計實現損益 (Realized PnL)", f"${final_pnl:,.1f}")

                # --- NEW CHART & STATS ---
                start_date = pd.to_datetime(filtered['date'].min())
                end_date = datetime.now()
                yf_code = sel_stock + '.TW' if not (sel_stock.endswith('.TW') or sel_stock.endswith('.TWO')) else sel_stock
                price_hist = yf.download(yf_code, start=start_date.strftime("%Y-%m-%d"), end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"), progress=False)
                
                # Check current unrealized PnL for active stock
                current_shares = processed_df.iloc[-1]['accum_shares']
                avg_cost = processed_df.iloc[-1]['avg_cost']
                if current_shares > 0 and not price_hist.empty:
                    current_price = float(price_hist['Close'].iloc[-1].item() if isinstance(price_hist['Close'].iloc[-1], pd.Series) else price_hist['Close'].iloc[-1])
                    unrealized_pnl = (current_price - avg_cost) * current_shares
                    unrealized_pct = (unrealized_pnl / (avg_cost * current_shares) * 100) if avg_cost > 0 else 0
                    st.markdown(f"**未實現損益 (Unrealized PnL)**: `${unrealized_pnl:,.1f}` ({unrealized_pct:+.2f}%)")

                if not price_hist.empty:
                    fig_s = go.Figure()
                    
                    # Ensure price_hist['Close'] is a 1D array/series, yf might return multi-level dataframe depending on version
                    if isinstance(price_hist.columns, pd.MultiIndex):
                        close_series = price_hist['Close'].droplevel(1, axis=1).squeeze()
                        if isinstance(close_series, pd.DataFrame):
                            close_series = close_series.iloc[:, 0]
                    else:
                        close_series = price_hist['Close']
                        if isinstance(close_series, pd.DataFrame):
                            close_series = close_series.iloc[:, 0]
                            
                    fig_s.add_trace(go.Scatter(x=price_hist.index, y=close_series, name='收盤價', line=dict(color='gray', dash='dot')))
                    
                    # Marker traces
                    df_marker = filtered.copy()
                    df_marker['date'] = pd.to_datetime(df_marker['date'])
                    
                    buys = df_marker[df_marker['action'].isin(['Buy', 'Add', 'Setup'])]
                    sells = df_marker[df_marker['action'].isin(['Reduce', 'Close'])]
                    
                    if not buys.empty:
                        fig_s.add_trace(go.Scatter(x=buys['date'], y=buys['price'], mode='markers', name='建立/加碼庫存', marker=dict(color='#22c55e', symbol='triangle-up', size=14, line=dict(width=1, color='white'))))
                    if not sells.empty:
                        fig_s.add_trace(go.Scatter(x=sells['date'], y=sells['price'], mode='markers', name='減碼/平倉', marker=dict(color='#ef4444', symbol='triangle-down', size=14, line=dict(width=1, color='white'))))
                        
                    fig_s.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
                    fig_s.update_layout(template='plotly_dark', margin=dict(t=20, b=20, l=10, r=10), height=350)
                    st.plotly_chart(fig_s, use_container_width=True)
                else:
                    st.warning("無法載入歷史股價走勢，可能是代號格式問題。")

                st.markdown("##### 詳細操作歷程")
                for idx, row in processed_df.iterrows():'''

code = re.sub(old_pnl, new_pnl, code, flags=re.DOTALL)

with open("app.py", "w") as f:
    f.write(code)

print("Done updating!")
