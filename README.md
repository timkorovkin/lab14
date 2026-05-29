Лабораторная работа 14 Студент: Коровкин Тимофей Ильич Группа: 220032-11 Вариант: 8 Сложность: средняя

## Архитектура конвейера
Binance WebSocket
(BTCUSDT, ETHUSDT, BNBUSDT)
│
▼
┌─────────────────┐
│   Go-сборщик    │  горутины + буферизация + graceful shutdown
└─────────────────┘
│
▼
┌─────────────────┐
│  JSON (JSONL)   │  data/trades.jsonl
└─────────────────┘
│
▼
┌─────────────────┐
│     Polars      │  очистка, типизация, агрегация
└─────────────────┘
│
▼
┌─────────────────┐
│     Parquet     │  data/trades.parquet (сжатие 15x)
└─────────────────┘
│
▼
┌─────────────────┐
│     DuckDB      │  SQL-аналитика прямо по Parquet
└─────────────────┘
│
▼
┌─────────────────┐
│     Plotly      │  интерактивные HTML-графики
└─────────────────┘

---

## Структура репозитория
lab14/
├── collector/
│   ├── main.go
│   └── go.mod
├── data/
│   ├── trades.jsonl
│   ├── trades_clean.jsonl
│   └── trades.parquet
├── analysis/
│   ├── task4_import.py
│   ├── task5_clean.py
│   ├── task6_aggregation.py
│   ├── task7_parquet.py
│   ├── task8_duckdb.py
│   ├── task9_visualization.py
│   └── requirements.txt
└── charts/
├── chart1_candlestick.html
├── chart2_distribution.html
└── chart3_volume.html

---

## Инструкция по запуску

### 1. Go-сборщик

```bash
cd collector
go mod tidy
go run main.go
# Работает до Ctrl+C — данные пишутся в data/trades.jsonl
```

### 2. Python-анализ

```bash
cd analysis
pip install -r requirements.txt

python task4_import.py
python task5_clean.py
python task6_aggregation.py
python task7_parquet.py
python task8_duckdb.py
python task9_visualization.py
```

---

## Примеры SQL-запросов (DuckDB)

```sql
-- Агрегация по торговой паре
SELECT
    symbol,
    COUNT(*)                        AS trade_count,
    ROUND(AVG(price), 4)            AS avg_price,
    ROUND(SUM(price * quantity), 2) AS volume_usd
FROM read_parquet('data/trades.parquet')
WHERE price > 0
GROUP BY symbol
ORDER BY volume_usd DESC;

-- OHLCV свечи по минутам для BTCUSDT
SELECT
    date_trunc('minute', timestamp) AS minute,
    FIRST(price ORDER BY timestamp) AS open,
    MAX(price)                      AS high,
    MIN(price)                      AS low,
    LAST(price ORDER BY timestamp)  AS close,
    SUM(quantity)                   AS volume
FROM read_parquet('data/trades.parquet')
WHERE symbol = 'BTCUSDT'
GROUP BY 1
ORDER BY 1;
```

---

## Результаты

### Собранные данные
- 1721 сделка за ~10 минут наблюдения
- Торговые пары: BTCUSDT, ETHUSDT, BNBUSDT
- Сжатие JSONL → Parquet: 15.5x

### Агрегация по символу
| symbol  | trades | avg_price | volume_usd |
|---------|--------|-----------|------------|
| BTCUSDT | 622    | 73 420 $  | 304 459 $  |
| ETHUSDT | 1039   | 2 013 $   | 112 954 $  |
| BNBUSDT | 60     | 641 $     | 10 583 $   |

### Сравнение производительности DuckDB vs Polars
| Запрос               | DuckDB   | Polars  |
|----------------------|----------|---------|
| Агрегация по символу | 3414 мс* | 3.26 мс |
| Buy/sell баланс      | 4.86 мс  | 1.56 мс |
| OHLCV свечи          | 4.74 мс  | 1.34 мс |

*холодный старт DuckDB (инициализация движка), последующие запросы 4-5 мс

### Графики
- `chart1_candlestick.html` — минутные свечи всех трёх пар
- `chart2_distribution.html` — гистограмма распределения цен
- `chart3_volume.html` — объём торгов и доля сделок по парам