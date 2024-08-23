from flask import Flask, request, jsonify
from datetime import datetime
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.date_processing import process_data, prepare_dataframe_for_insert

import pandas as pd

create_bucket_if_not_exists("novo-bucket")

execute_sql_script('sql/create_table.sql')

def receive_data():

        # Assuming the CSV file is sent in the request
        data = pd.read_csv('dados/sku_price.csv', encoding='latin1')

        # Process and save data
        filename = process_data(data)
        upload_file("novo-bucket", filename)

        # Read Parquet file from MinIO
        download_file("novo-bucket", filename, f"downloaded_{filename}")
        df_parquet = pd.read_parquet(f"downloaded_{filename}")

        # Prepare and insert data into ClickHouse
        df_prepared = prepare_dataframe_for_insert(df_parquet)
        client = get_client()  # Get ClickHouse client
        insert_dataframe(client, 'working_data', df_prepared)

execute_sql_script('sql/create_view.sql')

if __name__ == '__main__':
    receive_data()