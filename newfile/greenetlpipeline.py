import psycopg2
import os
from sqlalchemy import create_engine
import time 
import argparse
import pandas as pd

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    url2 = params.url2

    # Download the Parquet file directly from the provided URL
    if url.endswith('.parquet'):
        os.system(f"wget {url} -O {table_name}.parquet")
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(f"{table_name}.parquet")
        df.to_csv(f"{table_name}.csv", index=False)
        csv_name = f"{table_name}.csv"
    else:
        csv_name = 'output.csv'
    
    # Convert date columns to datetime
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Create engine connection to PostgreSQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    try:
        connection = engine.connect()
        print('Connection successful')
    except Exception as e:
        print(f'Error: {e}')

    # Print schema of the DataFrame
    print(pd.io.sql.get_schema(df, 'green_tripdata', con=engine))

    # Write DataFrame's structure to SQL
    df.head(0).to_sql('green_tripdata', con=engine, if_exists='replace')

    # Read CSV in chunks and load it into the database
    try:
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000)
        number = 0
        start = time.time()

        for chunk in df_iter:
            if isinstance(chunk, pd.DataFrame):
                chunk.to_sql('green_tripdata', con=engine, if_exists='append', index=False)
                number += 1

        end = time.time()
        print(f"Loaded {number} chunks into the database.")
        print(f"Total time taken: {end - start:.2f} seconds.")
    except Exception as e:
        print("An error occurred:", str(e))

    # Download and process the lookup table (CSV)
    os.system(f"wget {url2} -O taxi_zone_lookup.csv")
    lookup_table = pd.read_csv('taxi_zone_lookup.csv')
    lookup_table.to_sql('lookup_table', con=engine, if_exists='replace')

    # Perform join and store in a new table
    query = """
        SELECT 
        yt."VendorID",
        yt."lpep_pickup_datetime", 
        yt."lpep_dropoff_datetime",
        yt."store_and_fwd_flag",
        yt."RatecodeID",
        yt."PULocationID",
        yt."DOLocationID", 
        yt."passenger_count",
        yt."trip_distance", 
        yt."fare_amount", 
        yt."extra",
        yt."mta_tax", 
        yt."tip_amount", 
        yt."tolls_amount", 
        yt."ehail_fee", 
        yt."improvement_surcharge",
        yt."total_amount", 
        yt."payment_type", 
        yt."trip_type", 
        yt."congestion_surcharge",
        lt."Borough",
        lt."Zone", 
        lt."service_zone"
        FROM green_tripdata yt
        JOIN lookup_table lt
        ON yt."PULocationID" = lt."LocationID"
        AND yt."DOLocationID" = lt."LocationID"
        WHERE lt."Borough" IS NOT NULL
    """
    
    df_joined = pd.read_sql(query, con=engine)
    df_joined.to_sql('green_trip_fact_table', con=engine, if_exists='replace')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument('--table_name', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--url2', required=True)
    args = parser.parse_args()

    main(args)

