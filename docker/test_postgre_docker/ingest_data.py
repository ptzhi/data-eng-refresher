# script to ingest data into postgres docker

import os
import argparse
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):
	username = params.username
	password = params.password
	host = params.host
	port = params.port
	database = params.database
	table = params.table

	#transform parquet data to df
	trips_pq = pq.read_table('ny_taxi_local_data/yellow_tripdata_2021-01.parquet')
	trips = trips_pq.to_pandas()

	#connect to postgresql docker
	# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

	engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

	#write in chunks to db
	n=100000
	trips_chunks = [trips[i:i+n] for i in range(0, trips.shape[0], n)]

	for n in range(0, len(trips_chunks)):
		trips_chunks[n].to_sql(name=table, con=engine, if_exists='append')
		print(f'chunk {n+1}/{len(trips_chunks)+1} complete')

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Ingest data script')
	parser.add_argument('--username', type=str, help='username for postgres')
	parser.add_argument('--password', type=str, help='password for postgres')
	parser.add_argument('--host', type=str, help='host for docker, usually localhost')
	parser.add_argument('--port', type=str, help='port for docker postgres, usually 5432')
	parser.add_argument('--database', type=str, help='postgres database name')
	parser.add_argument('--table', type=str, help='postgres table name')

	args = parser.parse_args()
	main(args)
