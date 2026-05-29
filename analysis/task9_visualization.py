import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path

PARQUET_FILE = Path("../data/trades.parquet")
CHARTS_DIR   = Path("../charts")

def load(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)

# ─────────────────────────────────────────────
# График 1: Временной ряд цен (OHLCV свечи)
# ─────────────────────────────────────────────

def chart_price_timeseries(df: pl.DataFrame) -> None:
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=("BTCUSDT", "ETHUSDT", "BNBUSDT"),
        row_heights=[0.4, 0.4, 0.2],
    )

    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    colors  = ["#00d4aa", "#627eea", "#f0b90b"]

    for i, (sym, color) in enumerate(zip(symbols, colors), start=1):
        sub = (
            df.filter(pl.col("symbol") == sym)
            .with_columns(pl.col("timestamp").dt.truncate("1m").alias("minute"))
            .group_by("minute")
            .agg([
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("close"),
                pl.col("quantity").sum().alias("volume"),
            ])
            .sort("minute")
            .to_pandas()
        )

        fig.add_trace(
            go.Candlestick(
                x=sub["minute"],
                open=sub["open"], high=sub["high"],
                low=sub["low"],   close=sub["close"],
                name=sym,
                increasing_line_color=color,
                decreasing_line_color="#ff4d4d",
            ),
            row=i, col=1,
        )

    fig.update_layout(
        title="Минутные свечи криптовалют (Binance WebSocket)",
        template="plotly_dark",
        height=700,
        showlegend=True,
        xaxis_rangeslider_visible=False,
    )

    out = CHARTS_DIR / "chart1_candlestick.html"
    fig.write_html(str(out))
    print(f"[1] Сохранён: {out}")

# ─────────────────────────────────────────────
# График 2: Гистограмма распределения цен
# ─────────────────────────────────────────────

def chart_price_distribution(df: pl.DataFrame) -> None:
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    )

    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    colors  = ["#00d4aa", "#627eea", "#f0b90b"]

    for i, (sym, color) in enumerate(zip(symbols, colors), start=1):
        prices = df.filter(pl.col("symbol") == sym)["price"].to_list()
        fig.add_trace(
            go.Histogram(
                x=prices,
                nbinsx=30,
                name=sym,
                marker_color=color,
                opacity=0.85,
            ),
            row=1, col=i,
        )

    fig.update_layout(
        title="Распределение цен сделок по торговым парам",
        template="plotly_dark",
        height=420,
        showlegend=True,
        bargap=0.05,
    )

    out = CHARTS_DIR / "chart2_distribution.html"
    fig.write_html(str(out))
    print(f"[2] Сохранён: {out}")

# ─────────────────────────────────────────────
# График 3: Объём торгов по парам (bar + pie)
# ─────────────────────────────────────────────

def chart_volume_comparison(df: pl.DataFrame) -> None:
    agg = (
        df.group_by("symbol")
        .agg([
            pl.len().alias("trade_count"),
            (pl.col("price") * pl.col("quantity")).sum().round(2).alias("volume_usd"),
            pl.col("quantity").sum().round(4).alias("total_qty"),
        ])
        .sort("volume_usd", descending=True)
        .to_pandas()
    )

    colors = ["#00d4aa", "#627eea", "#f0b90b"]

    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "bar"}, {"type": "pie"}]],
        subplot_titles=["Объём торгов (USD)", "Доля сделок по парам"],
    )

    # Bar chart — объём в USD
    fig.add_trace(
        go.Bar(
            x=agg["symbol"],
            y=agg["volume_usd"],
            marker_color=colors,
            text=agg["volume_usd"].apply(lambda v: f"${v:,.0f}"),
            textposition="outside",
            name="Volume USD",
        ),
        row=1, col=1,
    )

    # Pie chart — количество сделок
    fig.add_trace(
        go.Pie(
            labels=agg["symbol"],
            values=agg["trade_count"],
            marker_colors=colors,
            hole=0.4,
            name="Trades",
        ),
        row=1, col=2,
    )

    fig.update_layout(
        title="Сравнение торговой активности по парам",
        template="plotly_dark",
        height=450,
        showlegend=True,
    )

    out = CHARTS_DIR / "chart3_volume.html"
    fig.write_html(str(out))
    print(f"[3] Сохранён: {out}")

# ─────────────────────────────────────────────

if __name__ == "__main__":
    CHARTS_DIR.mkdir(exist_ok=True)

    print("Загружаем данные...")
    df = load(PARQUET_FILE)
    print(f"Строк: {df.height}")

    print("\nСтроим графики...")
    chart_price_timeseries(df)
    chart_price_distribution(df)
    chart_volume_comparison(df)

    print("\nГотово. Открой файлы в браузере:")
    for f in sorted(CHARTS_DIR.glob("*.html")):
        print(f"  {f}")