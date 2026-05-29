import polars as pl
from pathlib import Path

CLEAN_FILE = Path("../data/trades_clean.jsonl")

def load_clean(path: Path) -> pl.DataFrame:
    return pl.read_ndjson(path).with_columns([
        pl.col("price").cast(pl.Float64),
        pl.col("quantity").cast(pl.Float64),
        pl.col("timestamp")
            .str.to_datetime("%Y-%m-%d %H:%M:%S%.3f")
            .alias("timestamp"),
    ])

def aggregate_by_symbol(df: pl.DataFrame) -> pl.DataFrame:
    """Группировка по торговой паре."""
    return (
        df.group_by("symbol")
        .agg([
            pl.col("price").sum().alias("volume_usd"),
            pl.col("price").mean().alias("avg_price"),
            pl.col("price").min().alias("min_price"),
            pl.col("price").max().alias("max_price"),
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("quantity").mean().alias("avg_quantity"),
            pl.len().alias("trade_count"),
        ])
        .sort("volume_usd", descending=True)
    )

def aggregate_by_side(df: pl.DataFrame) -> pl.DataFrame:
    """Группировка по стороне сделки (buyer/seller maker)."""
    return (
        df.with_columns(
            pl.when(pl.col("is_buyer_maker"))
              .then(pl.lit("sell"))   # buyer_maker=True означает sell-ордер исполнен
              .otherwise(pl.lit("buy"))
              .alias("side")
        )
        .group_by("symbol", "side")
        .agg([
            pl.len().alias("trade_count"),
            pl.col("quantity").sum().alias("total_quantity"),
            pl.col("price").mean().alias("avg_price"),
        ])
        .sort("symbol", "side")
    )

def aggregate_by_minute(df: pl.DataFrame) -> pl.DataFrame:
    """Группировка по минутам — OHLCV свечи."""
    return (
        df.with_columns(
            pl.col("timestamp").dt.truncate("1m").alias("minute")
        )
        .group_by("symbol", "minute")
        .agg([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("quantity").sum().alias("volume"),
            pl.len().alias("trades"),
        ])
        .sort("symbol", "minute")
    )

def print_table(title: str, df: pl.DataFrame) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print(df)

if __name__ == "__main__":
    print("Загружаем очищенные данные...")
    df = load_clean(CLEAN_FILE)
    print(f"Строк: {df.height}")

    agg_symbol = aggregate_by_symbol(df)
    print_table("АГРЕГАЦИЯ ПО ТОРГОВОЙ ПАРЕ", agg_symbol)

    agg_side = aggregate_by_side(df)
    print_table("АГРЕГАЦИЯ ПО ПАРЕ И СТОРОНЕ СДЕЛКИ (buy/sell)", agg_side)

    agg_minute = aggregate_by_minute(df)
    print_table("OHLCV СВЕЧИ ПО МИНУТАМ", agg_minute)
    print(f"\nВсего свечей: {agg_minute.height}")