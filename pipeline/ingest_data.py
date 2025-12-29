#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

year = 2021
month = 1
dtype = {
"VendorID": "Int64",
"passenger_count": "Int64",
"trip_distance": "float64",
"RatecodeID": "Int64",
"store_and_fwd_flag": "string",
"PULocationID": "Int64",
"DOLocationID": "Int64",
"payment_type": "Int64",
"fare_amount": "float64",
"extra": "float64",
"mta_tax": "float64",
"tip_amount": "float64",
"tolls_amount": "float64",
"improvement_surcharge": "float64",
"total_amount": "float64",
"congestion_surcharge": "float64"
}


parse_dates = [
"tpep_pickup_datetime",
"tpep_dropoff_datetime"
]
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
url = f'{prefix}//yellow_tripdata_{year}-{month:02d}.csv.gz'





@click.command()
@click.option('--pg-user', default='root', show_default=True, help='Postgres user')
@click.option('--pg-password', default='root', show_default=False, help='Postgres password')
@click.option('--pg-host', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', default='5432', show_default=True, help='Postgres port')
@click.option('--pg-db', default='ny_taxi', show_default=True, help='Postgres database name')
@click.option('--chunk-size', default=100000, type=int, show_default=True, help='CSV chunk size')
@click.option('--table-name', default='yellow_taxi_data', show_default=True, help='Destination table name')
@click.option('--year', default=2021, type=int, show_default=True, help='Year of the dataset')
@click.option('--month', default=1, type=int, show_default=True, help='Month of the dataset')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, chunk_size, table_name, year, month):

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}//yellow_tripdata_{year}-{month:02d}.csv.gz'

    df = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates)

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunk_size)

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='append'
        )


if __name__ == '__main__':
    run()

