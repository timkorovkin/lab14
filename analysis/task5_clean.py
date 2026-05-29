import polars as pl
from pathlib import Path

DATA_FILE = Path("../data/trades.jsonl")
CLEAN_FILE = Path("../data/trades_clean.jsonl")

def load_raw(path: Path) -> pl.DataFrame:
    return pl.read_ndjson(path)

def clean(df: pl.DataFrame) -> pl.DataFrame:

    original_count = df.height

    # Шаг 1: Удаление дубликатов по trade_id
    df = df.unique(subset=["trade_id"], keep="first")
    after_dedup = df.height
    print(f"[1] Дубликаты удалены: {original_count - after_dedup} строк убрано "
          f"({original_count} → {after_dedup})")

    # Шаг 2: Приведение price и quantity к Float64
    df = df.with_columns([
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col("quantity").cast(pl.Float64).alias("quantity"),
    ])
    print(f"[2] price и quantity приведены к Float64")

    # Шаг 3: Приведение timestamp (миллисекунды) к Datetime
    df = df.with_columns([
        (pl.col("timestamp") * 1_000)
            .cast(pl.Datetime("us"))
            .alias("timestamp")
    ])
    print(f"[3] timestamp приведён к Datetime (из миллисекунд Unix)")

    # Шаг 4: Приведение collected_at к Datetime
    df = df.with_columns([
        pl.col("collected_at")
            .str.to_datetime("%Y-%m-%dT%H:%M:%SZ")
            .alias("collected_at")
    ])
    print(f"[4] collected_at приведён к Datetime")

    # Шаг 5: Обработка пропусков после каст-операций
    null_after = df.null_count()
    total_nulls = sum(null_after[col][0] for col in df.columns)
    if total_nulls > 0:
        print(f"[5] Обнаружено {total_nulls} пропусков после приведения типов:")
        for col in df.columns:
            n = null_after[col][0]
            if n > 0:
                print(f"    - {col}: {n} пропусков → удаляем строки")
        df = df.drop_nulls()
        print(f"    Строк после удаления: {df.height}")
    else:
        print(f"[5] Пропусков после приведения типов не обнаружено ✅")

    # Шаг 6: Фильтрация аномалий — цена и количество должны быть > 0
    before = df.height
    df = df.filter(
        (pl.col("price") > 0) & (pl.col("quantity") > 0)
    )
    print(f"[6] Фильтрация аномалий (price > 0 и quantity > 0): "
          f"убрано {before - df.height} строк")

    # Шаг 7: Сортировка по времени
    df = df.sort("timestamp")
    print(f"[7] Данные отсортированы по timestamp")

    return df

def print_clean_info(df: pl.DataFrame) -> None:
    print("\n" + "=" * 50)
    print("ИТОГОВЫЙ ДАТАФРЕЙМ:")
    print("=" * 50)
    print(df.head(5))

    print("\n" + "=" * 50)
    print("ТИПЫ ПОСЛЕ ОЧИСТКИ:")
    print("=" * 50)
    for col, dtype in zip(df.columns, df.dtypes):
        print(f"  {col:<20} {dtype}")

    print("\n" + "=" * 50)
    print("РАСПРЕДЕЛЕНИЕ ПО СИМВОЛАМ:")
    print("=" * 50)
    print(df.group_by("symbol").len().sort("symbol"))

if __name__ == "__main__":
    print("Загружаем сырые данные...")
    df = load_raw(DATA_FILE)
    print(f"Загружено строк: {df.height}\n")

    print("Очистка данных:")
    print("-" * 40)
    df_clean = clean(df)

    print_clean_info(df_clean)

    # Сохраняем очищенный датафрейм для следующих заданий
    df_clean.write_ndjson(CLEAN_FILE)
    print(f"\nОчищенные данные сохранены в {CLEAN_FILE}")