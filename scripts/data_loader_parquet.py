import argparse
import json
from datetime import date, datetime

import pandas as pd
import requests

CHUNK_SIZE = 1000000  # Define the chunk size for processing

# Instantiate the parser
parser = argparse.ArgumentParser(description="Data Loader to delta table")
# Required positional argument
parser.add_argument(
    "--offset",
    type=int,
    default=0,
    help='Date offset in days to adjust the "day" and "Time" columns',
)


class timestamp_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


if __name__ == "__main__":
    args = parser.parse_args()

    print("Loading parquet file...")
    # Load the parquet file into a pandas DataFrame
    df = pd.read_parquet("~/Tmp/sensors", engine="pyarrow")

    # Change the column named day to curent date minus 1 day
    df["day"] = (pd.to_datetime(df["day"]) - pd.Timedelta(days=args.offset)).dt.date
    # CHange the column named Time to current timestamp minus 1 day
    df["Time"] = pd.to_datetime(df["Time"]) - pd.Timedelta(days=args.offset)

    print(df.head())

    for chunk_num in range(len(df) // CHUNK_SIZE + 1):
        start_index = chunk_num * CHUNK_SIZE
        end_index = min(chunk_num * CHUNK_SIZE + CHUNK_SIZE, len(df))
        chunk = df[start_index:end_index]

        json_payload = chunk.to_dict(orient="records")

        # call web api to send the chunk
        print(f"Chunk {chunk_num} processed with {len(chunk)} records.")
        # Here you would typically send the json_payload to your web API
        # For demonstration, we just print the first few lines

        payload = {
            "catalog_name": "main",
            "schema_name": "iot",
            "table_name": "sensors",
            "partition_by": ["day"],
            "values": json_payload,
        }

        r = requests.post(
            "http://localhost:8000/api/v1/data",
            data=json.dumps(payload, cls=timestamp_encoder),
        )
        if r.status_code == 204:
            print(f"Chunk {chunk_num} successfully sent.")
        else:
            print(f"Failed to send chunk {chunk_num}: {r.text}")
