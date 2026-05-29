import polars as pl
from pathlib import Path

DATA_FILE = Path("../data/trades.jsonl")

def load_trades(path: Path) -> pl.DataFrame:
    """Загружает JSONL-файл в Polars DataFrame."""
    df = pl.read_ndjson(path)
    return df

def print_basic_info(df: pl.DataFrame) -> None:
    """Выводит базовую информацию о DataFrame."""

    print("=" * 50)
    print("ПЕРВЫЕ 5 СТРОК:")
    print("=" * 50)
    print(df.head(5))

    print("\n" + "=" * 50)
    print("РАЗМЕР ДАННЫХ:")
    print("=" * 50)
    print(f"Строк:   {df.height}")
    print(f"Столбцов: {df.width}")

    print("\n" + "=" * 50)
    print("ТИПЫ ДАННЫХ:")
    print("=" * 50)
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"  {col:<20} {dtype}")

    print("\n" + "=" * 50)
    print("ПРОПУСКИ ПО СТОЛБЦАМ:")
    print("=" * 50)
    null_counts = df.null_count()
    for col in df.columns:
        n = null_counts[col][0]
        pct = n / df.height * 100
        status = "⚠️ " if n > 0 else "✅"
        print(f"  {status} {col:<20} {n} пропусков ({pct:.1f}%)")

    print("\n" + "=" * 50)
    print("СТАТИСТИКА ЧИСЛОВЫХ ПОЛЕЙ:")
    print("=" * 50)
    print(df.describe())

if __name__ == "__main__":
    if not DATA_FILE.exists():
        print(f"Файл не найден: {DATA_FILE}")
        exit(1)

    print(f"Загружаем данные из {DATA_FILE}...")
    df = load_trades(DATA_FILE)
    print_basic_info(df)