# stream data from parquet file to ppostgres database 
import os
from pydoc import text
from dotenv import load_dotenv
load_dotenv()
import pandas as pd         
from sqlalchemy import create_engine
from pathlib import Path






def load_data_to_db(parquet_dir, db_url, table_name):

    engine = create_engine(db_url)

    # 1. Read ALL parquet files
    parquet_files = sorted(Path(parquet_dir).glob("*.parquet"))

    if not parquet_files:
        print("No parquet files found.")
        return

    # 2. Load and combine all parquet data
    df = pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        ignore_index=True
    )

    # 3. Ensure datetime
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # 4. Get last loaded date from DB
    query = f"""
        SELECT MAX(date) as last_date
        FROM  staging.{table_name}
    """

    try:
        db_last_date = pd.read_sql(query, engine)["last_date"].iloc[0]
    except Exception:
        db_last_date = None

    print(f"DB last loaded date: {db_last_date}")

    # 5. Filter only new data
    if db_last_date:
        df = df[df["date"] > db_last_date]

    if df.empty:
        print("No new data to load. DB is up to date.")
        return

    # 6. Load to DB
    df.to_sql(
        table_name,
        schema="staging",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )
    engine.raw_connection().commit()

    print(f"Loaded {len(df)} new records into {table_name}")
if __name__ == "__main__":
    parquet_file = "/home/isaac/stock_market_pipeline/raw_data/"
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_port = os.getenv('DB_PORT')

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    table_name = "stg_stock_prices"

    load_data_to_db(parquet_file, db_url, table_name)

    




     

