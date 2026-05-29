import duckdb
import polars as pl
from pathlib import Path
import time

PARQUET_FILE = Path("../data/trades.parquet")

# ─────────────────────────────────────────────
# DuckDB — SQL-запросы к Parquet
# ─────────────────────────────────────────────

def duckdb_analysis(path: Path) -> dict:
    """Три SQL-запроса: фильтрация + группировка + сортировка."""
    con = duckdb.connect()
    results = {}

    # Запрос 1: агрегация по символу с фильтрацией по цене
    t0 = time.perf_counter()
    results["by_symbol"] = con.execute(f"""
        SELECT
            symbol,
            COUNT(*)                        AS trade_count,
            ROUND(AVG(price), 4)            AS avg_price,
            ROUND(MIN(price), 4)            AS min_price,
            ROUND(MAX(price), 4)            AS max_price,
            ROUND(SUM(quantity), 6)         AS total_qty,
            ROUND(SUM(price * quantity), 2) AS volume_usd
        FROM read_parquet('{path}')
        WHERE price > 0
          AND quantity > 0
        GROUP BY symbol
        ORDER BY volume_usd DESC
    """).df()
    t_duckdb_1 = time.perf_counter() - t0

    # Запрос 2: buy/sell баланс по символу
    t0 = time.perf_counter()
    results["buy_sell"] = con.execute(f"""
        SELECT
            symbol,
            CASE WHEN is_buyer_maker THEN 'sell' ELSE 'buy' END AS side,
            COUNT(*)                AS trade_count,
            ROUND(SUM(quantity), 6) AS total_qty,
            ROUND(AVG(price), 4)    AS avg_price
        FROM read_parquet('{path}')
        GROUP BY symbol, side
        ORDER BY symbol, side
    """).df()
    t_duckdb_2 = time.perf_counter() - t0

    # Запрос 3: OHLCV по минутам для BTCUSDT
    t0 = time.perf_counter()
    results["ohlcv"] = con.execute(f"""
        SELECT
            date_trunc('minute', timestamp)  AS minute,
            FIRST(price ORDER BY timestamp)  AS open,
            MAX(price)                       AS high,
            MIN(price)                       AS low,
            LAST(price ORDER BY timestamp)   AS close,
            ROUND(SUM(quantity), 6)          AS volume,
            COUNT(*)                         AS trades
        FROM read_parquet('{path}')
        WHERE symbol = 'BTCUSDT'
        GROUP BY date_trunc('minute', timestamp)
        ORDER BY minute
    """).df()
    t_duckdb_3 = time.perf_counter() - t0

    con.close()
    return results, t_duckdb_1, t_duckdb_2, t_duckdb_3

# ─────────────────────────────────────────────
# Polars — те же агрегации для сравнения
# ─────────────────────────────────────────────

def polars_analysis(path: Path) -> dict:
    df = pl.read_parquet(path)
    results = {}

    t0 = time.perf_counter()
    results["by_symbol"] = (
        df.filter((pl.col("price") > 0) & (pl.col("quantity") > 0))
        .group_by("symbol")
        .agg([
            pl.len().alias("trade_count"),
            pl.col("price").mean().round(4).alias("avg_price"),
            pl.col("price").min().alias("min_price"),
            pl.col("price").max().alias("max_price"),
            pl.col("quantity").sum().round(6).alias("total_qty"),
            (pl.col("price") * pl.col("quantity")).sum().round(2).alias("volume_usd"),
        ])
        .sort("volume_usd", descending=True)
    )
    t_polars_1 = time.perf_counter() - t0

    t0 = time.perf_counter()
    results["buy_sell"] = (
        df.with_columns(
            pl.when(pl.col("is_buyer_maker"))
              .then(pl.lit("sell"))
              .otherwise(pl.lit("buy"))
              .alias("side")
        )
        .group_by("symbol", "side")
        .agg([
            pl.len().alias("trade_count"),
            pl.col("quantity").sum().round(6).alias("total_qty"),
            pl.col("price").mean().round(4).alias("avg_price"),
        ])
        .sort("symbol", "side")
    )
    t_polars_2 = time.perf_counter() - t0

    t0 = time.perf_counter()
    results["ohlcv"] = (
        df.filter(pl.col("symbol") == "BTCUSDT")
        .with_columns(
            pl.col("timestamp").dt.truncate("1m").alias("minute")
        )
        .group_by("minute")
        .agg([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("quantity").sum().round(6).alias("volume"),
            pl.len().alias("trades"),
        ])
        .sort("minute")
    )
    t_polars_3 = time.perf_counter() - t0

    return results, t_polars_1, t_polars_2, t_polars_3

# ─────────────────────────────────────────────
# Вывод
# ─────────────────────────────────────────────

def print_section(title: str, df) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("=" * 60)
    print(df.to_string() if hasattr(df, "to_string") else df)

def print_comparison(label: str, t_duck: float, t_polars: float) -> None:
    winner = "DuckDB" if t_duck < t_polars else "Polars"
    print(f"  {label:<30} DuckDB: {t_duck*1000:6.2f} мс   "
          f"Polars: {t_polars*1000:6.2f} мс   → {winner} быстрее")

if __name__ == "__main__":
    print("Запускаем DuckDB-анализ...")
    duck_results, td1, td2, td3 = duckdb_analysis(PARQUET_FILE)

    print("Запускаем Polars-анализ...")
    pol_results, tp1, tp2, tp3 = polars_analysis(PARQUET_FILE)

    print_section("DuckDB — агрегация по символу", duck_results["by_symbol"])
    print_section("DuckDB — buy/sell баланс",      duck_results["buy_sell"])
    print_section("DuckDB — OHLCV свечи BTCUSDT",  duck_results["ohlcv"])

    print(f"\n{'='*60}")
    print("  СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 60)
    print_comparison("Агрегация по символу", td1, tp1)
    print_comparison("Buy/sell баланс",      td2, tp2)
    print_comparison("OHLCV свечи",          td3, tp3)

    print(f"\n  Итого DuckDB:  {(td1+td2+td3)*1000:.2f} мс")
    print(f"  Итого Polars:  {(tp1+tp2+tp3)*1000:.2f} мс")
    print(f"\n  Примечание: на малых данных (<10k строк) разница несущественна.")
    print(f"  DuckDB выигрывает на больших объёмах (>1M строк) за счёт")
    print(f"  векторизованного выполнения SQL прямо по Parquet без загрузки в память.")