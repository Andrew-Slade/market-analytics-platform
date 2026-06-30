from fastapi import FastAPI
import duckdb
from datetime import datetime
import yaml

__sub_file = "backend/config/subscription.yml"

app = FastAPI()
conn = duckdb.connect()
def reload_view():
    conn.execute("""
    CREATE OR REPLACE VIEW market_view AS
    SELECT *
    FROM read_parquet('/home/aslade/personal_projects/market-analytics-platform/data/**/*.snappy.parquet');
    """)

@app.get("/high_low")
async def highlow():
    pass #TODO return high and low

@app.get("/latest")
async def latest():
    pass

@app.get("/quickview")
async def marketview():
    reload_view()
    res = conn.sql(f"""
    SELECT product_id, SUM(last_size) as cumulative_volume, SUM(price * last_size) / SUM(last_size) as vwap
    FROM market_view
    WHERE year = {datetime.now().year} AND month = {datetime.now().month} AND day = {datetime.now().day} group by product_id
    """).fetchall()
    records = {i[0]:{} for i in res}
    for i in res:
        records[i[0]] = {"cumulative volume": round(i[1],3), "vwap": round(i[2],2)}
    return records

@app.post("/vwap")
async def vwap():
    pass

@app.post("/volatility")
async def volatility():
    pass

@app.post("/correlation")
async def corr():
    pass

@app.get("/dataset")
async def dataset():
    """
    View overall symbols and their row counts
    """
    reload_view()
    res =  conn.sql(f"""
    SELECT distinct product_id, count(product_id) as row_count
    FROM market_view
    WHERE year = {datetime.now().year} AND month = {datetime.now().month} AND day = {datetime.now().day} group by product_id order by count(product_id) desc
    """).fetchall()

    return {i[0]:i[1] for i in res}

@app.post("/subscribe")
async def subscribe(tickers: list[str]):
    "Alter subscriptions"
    try:
        with open(__sub_file) as f:
            conf = yaml.safe_load(f)
        for i in tickers:
            conf["tickers"].append(i)
        conf["tickers"] = list(set(conf["tickers"]))
        with open(__sub_file, "w") as w:
            yaml.dump(conf, w)
        f.close()
    except Exception as e:
        return e
    return conf

@app.post("/unsubscribe")
async def unsubscribe(tickers: list[str]):
    "Alter subscriptions"
    try:
        with open(__sub_file) as f:
            conf = yaml.safe_load(f)
        for i in tickers:
            conf["tickers"].remove(i)
        conf["tickers"] = list(set(conf["tickers"]))
        with open(__sub_file, "w") as w:
            yaml.dump(conf, w)
        f.close()
    except Exception as e:
        return e
    return conf