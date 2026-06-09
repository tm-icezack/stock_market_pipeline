import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta
from pathlib import Path


tickers = ["AAL", "TSLA", "GOOGL"]


# -------------------------
# EXTRACT ONLY (RAW)
# -------------------------



def fetch_raw_data(tickers):

    end_date = datetime.today()
    start_date = end_date - timedelta(days=780)

    data = yf.download(
        tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=False,
        progress=False
    )

    if data.empty:
        raise ValueError("No data returned from yfinance")

    return data


# -------------------------
# LOAD (RAW SAVE) AND SAVE IN A DAILY FORMAT
#check path and loads only missing  days
# -------------------------


def load_raw_data(df, output_dir):

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df["date"] = pd.to_datetime(df["date"])

    for trade_date, daily_df in df.groupby(df["date"].dt.date):

        file_path = output_dir / f"{trade_date}.parquet"

        if file_path.exists():
            print(f"Skipping {trade_date} - already exists")
            continue

        daily_df.to_parquet(file_path, index=False)

        print(f"Saved {file_path}")

# -------------------------
# MAIN PIPELINE
# -------------------------
def main():

    start_time = time.time()

    # Extract only
    raw_data = fetch_raw_data(tickers)

    print("\nData preview:")
    print(raw_data.head())

    # Convert MultiIndex columns to rows
    raw_data2 = (raw_data.stack(level="Ticker")
               .reset_index())
              
    # add ingestion timestamp and data source columns
    raw_data2['ingestion_timestamp'] = pd.Timestamp.now()
    raw_data2['data_source'] = 'yfinance'
    # convert all columns to SQL-friendly names
    raw_data2.columns = (
    raw_data2.columns
    .str.strip()
    .str.lower()
    .str.replace(' ', '_'))

    print(raw_data2.head()) 


    # Save raw
    #output_path = "/home/isaac/stock_market_pipeline/raw_data/raw_stock_data.parquet"
    output_dir = "/home/isaac/stock_market_pipeline/raw_data/"
    
    load_raw_data(raw_data2, output_dir)

    


    end_time = time.time()

    print("\nPipeline completed successfully!")
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    files = sorted(Path(output_dir).glob("*.parquet"))

    print(f"Successfully saved {len(files)} parquet files")

    for f in files:
        print(f.name)
if __name__ == "__main__":
    main()