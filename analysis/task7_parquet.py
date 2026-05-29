import polars as pl
from pathlib import Path
import time

CLEAN_FILE = Path("../data/trades_clean.jsonl")
PARQUET_FILE = Path("../data/trades.parquet")

def load_clean(path: Path) -> pl.DataFrame:
    return pl.read_ndjson(path).with_columns([
        pl.col("price").cast(pl.Float64),
        pl.col("quantity").cast(pl.Float64),
        pl.col("timestamp")
            .str.to_datetime("%Y-%m-%d %H:%M:%S%.3f")
            .alias("timestamp"),
    ])

def save_parquet(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path, compression="snappy")

def compare_sizes(json_path: Path, parquet_path: Path) -> None:
    json_size = json_path.stat().st_size
    parquet_size = parquet_path.stat().st_size
    ratio = json_size / parquet_size
    print(f"  JSONL:   {json_size:>10,} байт ({json_size/1024:.1f} KB)")
    print(f"  Parquet: {parquet_size:>10,} байт ({parquet_size/1024:.1f} KB)")
    print(f"  Сжатие:  {ratio:.2f}x — Parquet в {ratio:.1f} раз меньше")

def verify_parquet(path: Path) -> None:
    """Читаем обратно и проверяем что данные не потеряны."""
    df = pl.read_parquet(path)
    print(f"\n  Строк после чтения:   {df.height}")
    print(f"  Столбцов:             {df.width}")
    print(f"  Типы данных:")
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"    {col:<20} {dtype}")
    print(f"\n  Первые 3 строки:")
    print(df.head(3))

if __name__ == "__main__":
    print("Загружаем очищенные данные...")
    df = load_clean(CLEAN_FILE)
    print(f"Строк: {df.height}, столбцов: {df.width}")

    print(f"\nСохраняем в Parquet (compression=snappy)...")
    t0 = time.perf_counter()
    save_parquet(df, PARQUET_FILE)
    elapsed = time.perf_counter() - t0
    print(f"Записано за {elapsed*1000:.1f} мс")

    print(f"\n{'='*50}")
    print("СРАВНЕНИЕ РАЗМЕРОВ ФАЙЛОВ:")
    print("=" * 50)
    compare_sizes(CLEAN_FILE, PARQUET_FILE)

    print(f"\n{'='*50}")
    print("ВЕРИФИКАЦИЯ — ЧИТАЕМ PARQUET ОБРАТНО:")
    print("=" * 50)
    verify_parquet(PARQUET_FILE)

    print(f"\nParquet сохранён: {PARQUET_FILE.resolve()}")