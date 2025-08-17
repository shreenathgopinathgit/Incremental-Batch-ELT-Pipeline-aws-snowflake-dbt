import pandas as pd
import boto3
import json
import os
from datetime import datetime
from sqlalchemy import create_engine

# --------------------
# CONFIG
# --------------------
DB_USER = "admin"
DB_PASSWORD = "Shree0333"
DB_HOST = "ecomdatabase.cnic606qaeyr.ap-southeast-2.rds.amazonaws.com"
DB_PORT = "3306"
DB_NAME = "ecom_data"

BUCKET_NAME = "movielensdbtbucket"
S3_PREFIX = "ecom/raw"

TABLE_NAME = "olist_orders"
DATE_COL = "order_purchase_timestamp"

WATERMARK_FILE = "watermarks.json"

# --------------------
# Load & save watermarks locally
# --------------------
def load_watermarks():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            return json.load(f)
    return {}

def save_watermarks(watermarks):
    with open(WATERMARK_FILE, "w") as f:
        json.dump(watermarks, f)

# --------------------
# Main
# --------------------
def main():
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    s3 = boto3.client("s3")

    # Load last watermark or set default
    watermarks = load_watermarks()
    last_wm = watermarks.get(TABLE_NAME, "1970-01-01 00:00:00")

    # Incremental query with %% escaping
    query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE STR_TO_DATE({DATE_COL}, '%%Y-%%m-%%d %%H:%%i:%%s') 
              > STR_TO_DATE('{last_wm}', '%%Y-%%m-%%d %%H:%%i:%%s')
    """

    print(f"Running query:\n{query}")
    df = pd.read_sql(query, con=engine)

    if df.empty:
        print("No new rows found.")
        return

    # Save to S3 with date partition
    now = datetime.utcnow()
    key = f"{S3_PREFIX}/{TABLE_NAME}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{TABLE_NAME}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    csv_data = df.to_csv(index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_data)
    print(f"Uploaded {len(df)} rows to s3://{BUCKET_NAME}/{key}")

    # Update watermark
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    max_ts = df[DATE_COL].max()
    if pd.notnull(max_ts):
        watermarks[TABLE_NAME] = max_ts.strftime("%Y-%m-%d %H:%M:%S")
        save_watermarks(watermarks)
        print(f"Watermark updated to {watermarks[TABLE_NAME]}")

if __name__ == "__main__":
    main()