#!/usr/bin/env python
# coding: utf-8


import os
import argparse
import pyarrow.parquet as pq
import fastparquet
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = "http://localhost:8000/yellow_tripdata_2022-01.parquet"
    ##"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"

   
    ##params.url

    parquet_name = 'output.parquet'

    os.system(f"curl {url} -o {parquet_name}")
    
    table=pq.read_table(parquet_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    #df = pd.read_parquet(parquet_name)
    df= table.to_pandas()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest partquet into postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    ##parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)




