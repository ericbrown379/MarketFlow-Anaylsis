import os
import pathlib
import time
from typing import Optional

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text


def load_dotenv(env_path: Optional[str] = None):
    """Lightweight .env loader (doesn't require python-dotenv)."""
    if env_path is None:
        env_path = pathlib.Path(__file__).resolve().parents[1] / 'spark_streaming' / '.env'
    p = pathlib.Path(env_path)
    if not p.exists():
        return
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if os.environ.get(k) is None:
            os.environ[k] = v


def get_engine():
    load_dotenv()
    host = os.environ.get('PG_HOST', 'localhost')
    port = os.environ.get('PG_PORT', '5432')
    db = os.environ.get('PG_DB', 'crypto_metrics')
    user = os.environ.get('PG_USER', 'ericbrown')
    password = os.environ.get('PG_PASSWORD', '')

    # Build SQLAlchemy URL
    if password:
        url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    else:
        url = f'postgresql+psycopg2://{user}@{host}:{port}/{db}'

    engine = create_engine(url)
    return engine


@st.cache_data(ttl=15)
def load_latest_metrics(limit=500):
    engine = get_engine()
    query = text("SELECT * FROM crypto_table ORDER BY timestamp DESC LIMIT :limit")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"limit": limit})
    return df


@st.cache_data(ttl=30)
def load_top_tables():
    engine = get_engine()
    with engine.connect() as conn:
        gain_df = pd.read_sql(text("SELECT * FROM top_5_gainers ORDER BY rank"), conn)
        loss_df = pd.read_sql(text("SELECT * FROM top_5_losers ORDER BY rank"), conn)
    return gain_df, loss_df


def decision_rule(row, sma_window=5, change_threshold=0.5):
    # Simple rule-based decision: compare price to sma and change_5min against threshold
    # Returns a short decision string and explanation
    try:
        change = row.get('change_5min')
        price = float(row.get('price'))
        sma = float(row.get('sma'))
    except Exception:
        return 'HOLD', 'Insufficient data'

    if change is None:
        return 'HOLD', 'No 5-minute change available'

    # Buy condition: positive momentum and price above SMA
    if change >= change_threshold and price > sma:
        return 'BUY', f'change_5min={change:.2f}% >= {change_threshold} and price ({price:.2f}) > SMA ({sma:.2f})'

    # Sell condition: negative momentum and price below SMA
    if change <= -change_threshold and price < sma:
        return 'SELL', f'change_5min={change:.2f}% <= -{change_threshold} and price ({price:.2f}) < SMA ({sma:.2f})'

    return 'HOLD', f'change_5min={change:.2f} within [-{change_threshold},{change_threshold}] or price vs SMA ambiguous'


def main():
    st.set_page_config(page_title='Crypto Metrics Dashboard', layout='wide')
    st.title('Crypto Metrics — Stream & Signals')

    # Sidebar: controls
    st.sidebar.header('Connection & Controls')
    st.sidebar.write('Postgres connection is read from environment or spark_streaming/.env')
    refresh = st.sidebar.button('Refresh now')
    limit = st.sidebar.number_input('Rows to load', min_value=100, max_value=5000, value=1000, step=100)
    change_threshold = st.sidebar.slider('Change (5min) threshold (%)', min_value=0.1, max_value=5.0, value=0.5, step=0.1)

    # Load data
    with st.spinner('Loading latest metrics...'):
        df = load_latest_metrics(limit=limit)
        gain_df, loss_df = load_top_tables()

    if df.empty:
        st.warning('No metrics available in the database. Make sure the analytics job has written parquet -> Postgres rows.')
        return

    # Overview cards
    latest_ts = df['timestamp'].max()
    st.markdown(f'**Latest ingestion time:** {latest_ts}')

    col1, col2 = st.columns([3, 1])
    with col2:
        st.subheader('Top 5 Gainers')
        st.table(gain_df)
        st.subheader('Top 5 Losers')
        st.table(loss_df)

    # Interactive coin selector
    coins = sorted(df['id'].unique())
    selected_coin = st.selectbox('Select coin', coins, index=coins.index('bitcoin') if 'bitcoin' in coins else 0)

    coin_df = df[df['id'] == selected_coin].sort_values('timestamp')

    st.subheader(f'Time series — {selected_coin}')
    fig = px.line(coin_df, x='timestamp', y=['price', 'sma', 'ema'], labels={'value': 'USD', 'variable': 'metric'})
    st.plotly_chart(fig, use_container_width=True)

    st.subheader('Volatility (recent)')
    fig2 = px.line(coin_df, x='timestamp', y='volatility', labels={'volatility': 'Std Dev'})
    st.plotly_chart(fig2, use_container_width=True)

    # Show latest row and decision
    latest_row = coin_df.iloc[-1].to_dict()
    st.subheader('Latest metrics & decision')
    st.json(latest_row)

    decision, explanation = decision_rule(latest_row, change_threshold=change_threshold)
    if decision == 'BUY':
        st.success(f'RECOMMENDATION: {decision}')
    elif decision == 'SELL':
        st.error(f'RECOMMENDATION: {decision}')
    else:
        st.info(f'RECOMMENDATION: {decision}')
    st.write(explanation)

    st.markdown('---')
    st.markdown('Decision logic (simplified):')
    st.markdown(
        """
- BUY: 5-minute % change >= threshold AND price > SMA
- SELL: 5-minute % change <= -threshold AND price < SMA
- HOLD: otherwise
"""
    )

    # auto-refresh if user pressed refresh
    if refresh:
        st.experimental_rerun()


if __name__ == '__main__':
    main()
