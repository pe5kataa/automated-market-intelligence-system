from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import os

MARKET_INTEL_PY = os.getenv("MARKET_INTEL_PY")
PROJECT         = os.getenv("PROJECT")
DBT             = os.getenv("DBT")

@dag(dag_id="daily_stock_upload",start_date=datetime(2026, 1, 1), schedule="@daily", catchup=True)
def run_daily_price_pipeline():
    
    @task.external_python(
        python=MARKET_INTEL_PY,
        skip_on_exit_code=99,                 # exit 99 → mark task SKIPPED (no data day)
        retries=30,
        retry_delay=timedelta(minutes=1),     # retries are for transient errors (rate limits)
    )
    def extract(ds):
        import sys
        from ingestion.stocks_extract import extract_prices
        from datetime import date, timedelta

        tickers = ["AAPL", "MSFT", "NVDA", "TSLA"]

        print(ds)
        end_date = (date.fromisoformat(ds) + timedelta(days=1))

        df = extract_prices(tickers, ds, end_date)

        # yfinance NaN-fills rows when rate-limited → drop rows with no real price
        df = df.dropna()

        '''if df.empty:
            if run_date.weekday() >= 5:
                sys.exit(99) 
            else:
                raise RuntimeError(f"yfinance rate-limit -> will retry in 1 minutes")'''
        df.to_csv("/tmp/stocks.csv", index=False)
        return "/tmp/stocks.csv"

    @task.external_python(python=MARKET_INTEL_PY)
    def load(path):
        from ingestion.stocks_load import load_stock_prices
        import pandas as pd
        df = pd.read_csv(path)
        load_stock_prices(df)
        
    @task.external_python(python=MARKET_INTEL_PY)
    def create_raw_table():
        # Ensure schema and table are created
        from sqlalchemy import text
        from sql.create_raw_stocks_table import metadata, engine
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        metadata.create_all(engine)

    @task.bash()
    def run_dbt() -> str:
        return (f"cd '{PROJECT}/market_intel' && "
                f"{DBT} build")


    create_raw_table() >> load(extract()) >> run_dbt()
    
run_daily_price_pipeline()