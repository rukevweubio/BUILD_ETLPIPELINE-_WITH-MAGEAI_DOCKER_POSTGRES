#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import time
import argparse
from sqlalchemy import create_engine

def main(params):
    """
    Main function to download a parquet file, convert it to CSV, and load the data into PostgreSQL.
    
    Parameters:
    params (argparse.Namespace): Command line arguments.
    """
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    url2 = params.url2  # Make sure you want to use this variable

    # Download the Parquet file
    if url.endswith('.parquet'):
        os.system(f"wget {url} -O {table_name}.parquet")
        if os.path.exists(f"{table_name}.parquet"):
            df = pd.read_parquet(f"{table_name}.parquet")
            df.to_csv(f"{table_name}.csv", index=False)
            csv_name = f"{table_name}.csv"
            print(f"Downloaded and converted {url} to {csv_name}")
        else:
            print(f"Failed to download {url}. Exiting...")
            return
    else:
        csv_name = 'output.csv'
    
    # Load and preprocess data
    df = df.head(100000)  # Consider changing this limit based on your needs
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    try:
        with engine.connect() as connection:
            print("Connection successful!")

            # Show schema and create the table in the database
            print(pd.io.sql.get_schema(df, table_name))
            df.head(0).to_sql(table_name, con=connection, if_exists='replace', index=False)

            # Load data in chunks
            df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000)
            number = 0
            start = time.time()

            for chunk in df_iter:
                if isinstance(chunk, pd.DataFrame):  # Check if the chunk is a DataFrame
                    chunk.to_sql(table_name, con=connection, if_exists='append', index=False)
                    number += 1  # Increment the chunk counter

            # Calculate the time taken
            end = time.time()
            print(f"Loaded {number} chunks into the database.")
            print(f"Total time taken: {end - start:.2f} seconds.")

    except Exception as e:
        print("An error occurred while connecting or loading data:", str(e))

    # Load lookup table if needed
    try:
        os.system(f"wget {url2} -O taxi_zone_lookup.csv")
        if os.path.exists('taxi_zone_lookup.csv'):
            lookup_table = pd.read_csv('taxi_zone_lookup.csv')
            lookup_table.to_sql('lookup_table', con=engine, if_exists='replace')
            print("Lookup table loaded successfully.")
        else:
            print(f"Failed to download {url2}. Exiting...")
            return

        # SQL Query execution
        query = """
            SELECT 
            yt."VendorID",
            yt."tpep_pickup_datetime", 
            yt."tpep_dropoff_datetime",
            yt."passenger_count", 
            yt."trip_distance", 
            yt."RatecodeID",
            yt."store_and_fwd_flag",
            yt."PULocationID",
            yt."DOLocationID", 
            yt."payment_type", 
            yt."fare_amount", 
            yt."extra",
            yt."mta_tax", 
            yt."tip_amount", 
            yt."tolls_amount", 
            yt."improvement_surcharge",
            yt."total_amount", 
            yt."congestion_surcharge", 
            lt."Borough",
            lt."Zone", 
            lt."service_zone"
            FROM yellow_tripdata yt
            JOIN lookup_table lt
            ON yt."PULocationID" = lt."LocationID"
            AND yt."DOLocationID" = lt."LocationID"
            WHERE lt."Borough" IS NOT NULL
        """
        df_query_result = pd.read_sql(query, con=engine)
        df_query_result.to_sql('yellow_fact_table', con=engine, if_exists='replace')
        print("Query executed successfully and results loaded into yellow_fact_table.")

    except Exception as e:
        print("An error occurred while executing the query:", str(e))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument('--table_name', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--url2', required=True)  # Added here as a required argument
    args = parser.parse_args()

    main(args)


