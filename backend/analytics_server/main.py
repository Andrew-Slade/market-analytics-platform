import os
from fastapi import FastAPI
import duckdb
from datetime import datetime, timedelta
import yaml
import re
from functools import lru_cache

__SUB_FILE = "/app/backend/config/subscription.yml"
__DATA_ROOT = DATA_DIR = os.getenv("DATA_DIRECTORY", "/app/data")

app = FastAPI()
conn = duckdb.connect()

conn.execute("INSTALL delta; LOAD delta;")

@app.get("/analytics/high_low")
async def highlow(symbol:str, timeframe: str = "day"):
    if re.fullmatch(r"[a-zA-Z0-9-]+", symbol):
        end_time = datetime.now()
        match timeframe:
            case "1h":
                start_time = end_time - timedelta(hours=1)
            case "30m":
                start_time = end_time - timedelta(minutes=30)
            case "15m":
                start_time = end_time - timedelta(minutes=15)
            case "5m":
                start_time = end_time - timedelta(minutes=5)
            case _:
                start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
        symbols = [timeframe, datetime.now().year, datetime.now().month, datetime.now().day, symbol, start_time, end_time]
        res =  conn.execute(f"""
            SELECT 
                product_id,
                ? as timeframe,
                MIN(price) as low,
                MAX(price) as high
            FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=true)
            WHERE year = ? AND month = ? AND day = ?
            AND product_id = ?
            AND time BETWEEN ? AND ?
            GROUP BY product_id
        """, symbols).fetchdf().dropna()
        return res.to_dict(orient="records")
    else:
        return {"ERROR": f"INVALID SYMBOL {symbol}"}

@app.get("/analytics/latest_prices")
async def latest(symbol: str,limit: int = 10):
    """
    View of most recent ticks per symbol
    """
    symbols = [datetime.now().year, datetime.now().month, datetime.now().day, symbol, limit]
    if re.fullmatch(r"[a-zA-Z0-9-]+", symbol):
        res =  conn.execute(f"""
        WITH sample AS (
            SELECT 
                product_id,
                price,
                recieved, 
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY time DESC) AS rn
            FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=true)
            WHERE year = ?
            AND month = ?
            AND day = ?
            AND product_id = ?
        )

        SELECT
            product_id, 
            price,
            recieved
        FROM sample
        where rn <= ?
        """, symbols).fetchdf().dropna()
        return res.to_dict(orient="records")
    else:
        return {"ERROR": f"INVALID SYMBOL {symbol}"}

@app.get("/analytics/quickvwap")
async def marketview():
    """
    VWAP over all available data for all available symbols for this date
    """
    res = conn.execute(f"""
    SELECT
        product_id, 
        SUM(last_size) as cumulative_volume,
        SUM(price * last_size) / SUM(last_size) as vwap
    FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=True)
    WHERE year = ? 
    AND month = ? 
    AND day = ? 
    GROUP BY product_id
    """, [datetime.now().year, datetime.now().month, datetime.now().day]).fetchall()
    records = {i[0]:{} for i in res}
    for i in res:
        records[i[0]] = {"cumulative volume": round(i[1],3), "vwap": round(i[2],2)}
    return records

@app.get("/analytics/vwap")
async def vwap(symbol: str, timeframe: str = "day"):
    """
    VWAP over a particular symbol with all available data
    """
    if re.fullmatch(r"[a-zA-Z0-9-]+", symbol):
        end_time = datetime.now()
        match timeframe:
            case "1h":
                start_time = end_time - timedelta(hours=1)
            case "30m":
                start_time = end_time - timedelta(minutes=30)
            case "15m":
                start_time = end_time - timedelta(minutes=15)
            case "5m":
                start_time = end_time - timedelta(minutes=5)
            case _:
                start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
        symbols = [timeframe, datetime.now().year, datetime.now().month, datetime.now().day, symbol, start_time, end_time]
        res =  conn.execute(f"""
            SELECT 
                product_id,
                ? as timeframe,
                SUM(price * last_size) / SUM(last_size) as vwap
            FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=true)
            WHERE year = ? AND month = ? AND day = ?
            AND product_id = ?
            AND time BETWEEN ? AND ?
            GROUP BY product_id
        """, symbols).fetchdf().dropna()
        return res.to_dict(orient="records")
    else:
        return {"ERROR": f"INVALID SYMBOL {symbol}"}

@lru_cache(maxsize=128)
@app.get("/analytics/returns")
async def returns(symbol):
    symbols = [datetime.now().year, datetime.now().month, datetime.now().day, symbol]
    if re.fullmatch(r"[a-zA-Z0-9-]+", symbol):
        res =  conn.execute(f"""
        WITH resampled AS (
            SELECT 
                date_trunc('minute', time) AS bucket,
                arg_max(price, time) AS price
            FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=true)
            WHERE year = ?
            AND month = ?
            AND day = ?
            AND product_id = ?
            GROUP BY bucket
        )

        SELECT 
            bucket,
            price,
            ln(price / lag(price) OVER (ORDER BY bucket)) AS return
        FROM resampled
        ORDER BY bucket;
        """, symbols).fetchdf().dropna()
        return res.to_dict(orient="records")
    else:
        return {"ERROR": "INVALID SYMBOL"}

@lru_cache(maxsize=128)
@app.get("/analytics/volatility")
async def volatility(symbol: str):
    "Std deviation of returns and how far outside of that we are"
    symbols = [datetime.now().year, datetime.now().month, datetime.now().day, symbol, symbol]
    if re.fullmatch(r"[a-zA-Z0-9-]+", symbol):
        res =  conn.execute(f"""
        WITH resampled AS (
            SELECT 
                date_trunc('minute', time) AS bucket,
                arg_max(price, time) AS price
            FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=true)
            WHERE year = ?
            AND month = ?
            AND day = ?
            AND product_id = ?
            GROUP BY bucket
        ),
        returns AS (
            SELECT 
                bucket,
                price,
                LN(price / LAG(price) OVER (ORDER BY bucket)) AS log_returns
            FROM resampled
        )
        SELECT 
            ? as product_id,
            STDDEV(log_returns) as volatility
        FROM returns
        WHERE log_returns IS NOT NULL
        """, symbols).fetchdf().dropna()
        return res.to_dict(orient="records")
    else:
        return {"ERROR": "INVALID SYMBOL"}

@lru_cache(maxsize=128)
@app.get("/analytics/correlation")
async def corr(symbol1: str, symbol2: str):
    query  =[datetime.now().year, datetime.now().month, datetime.now().day, symbol1, symbol2, symbol1, symbol2, symbol1, symbol2]
    for i in query[3:]:
        if not isinstance(i, str) or not re.fullmatch(r"[a-zA-Z0-9-]+", i):
            return  {"ERROR": "INVALID SYMBOL"}

    res =  conn.execute(f"""
    WITH base as (
        SELECT
            product_id,
            date_trunc('minute', time) as bucket,
            arg_max(price, time) as price
        FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=True)
        WHERE year = ? AND month = ? AND day = ?
        AND product_id IN (?,?)
        GROUP BY product_id, bucket
    ),
    rets AS (
        SELECT
            product_id,
            bucket,
            LN(price/ LAG(price) OVER (PARTITION BY product_id ORDER BY bucket)) AS ret
        FROM base
    )
    SELECT
        ? as product_id_a,
        ? as product_id_b,
        corr(a.ret, b.ret) AS correlation
    FROM rets a
    JOIN rets b
        ON a.bucket = b.bucket
    WHERE a.product_id = ?
    AND b.product_id = ? AND a.ret IS NOT NULL AND b.ret IS NOT NULL
    """, query).fetchdf().dropna()
    return res.to_dict(orient="records")

@app.get("/analytics/dataset")
async def dataset():
    """
    View overall symbols and their row counts
    """
    res =  conn.execute(f"""
    SELECT 
        distinct product_id, 
        COUNT(product_id) AS row_count
    FROM read_parquet('{__DATA_ROOT}/**/*.snappy.parquet', hive_partitioning=True)
    WHERE year = ? 
    AND month = ? 
    AND day = ? 
    GROUP BY product_id 
    ORDER BY COUNT(product_id) DESC
    """, [datetime.now().year, datetime.now().month, datetime.now().day]).fetchall()

    return {i[0]:i[1] for i in res}


@app.post("/admin/subscribe")
async def subscribe(tickers: list[str]):
    "Alter subscriptions"
    with open(__SUB_FILE) as f:
        conf = yaml.safe_load(f)
    for i in tickers:
        conf["tickers"].append(str(i))
    conf["tickers"] = list(set(conf["tickers"]))
    with open(__SUB_FILE, "w") as w:
        yaml.dump(conf, w)
    f.close()
    return conf

@app.post("/admin/unsubscribe")
async def unsubscribe(tickers: list[str]):
    "Alter subscriptions"
    with open(__SUB_FILE) as f:
        conf = yaml.safe_load(f)
    for i in tickers:
        conf["tickers"].remove(str(i))
    conf["tickers"] = list(set(conf["tickers"]))
    with open(__SUB_FILE, "w") as w:
        yaml.dump(conf, w)
    f.close()
    return conf