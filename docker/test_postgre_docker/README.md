# Ingest into PostgreSQL with Docker

Ingest local data into PostgreSQL and query with pgcli or pgAdmin, all done through Docker from the terminal (e.g. no Airflow) so that the process is lightweight, consistent, and shareable.

## Instructions

### 1. Create Docker project folder

Create project folder and within it, an empty postgre folder to link to postgre in Docker container, and folder that will store local data to link to the container. Use whatever data, or take taxi data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page, e.g. `yellow_tripdata_2021-01.parquet` and put it in `ny_taxi_local_data`
~~~
$mkdir postgre_docker
$cd postgre_docker
$mkdir ny_taxi_local_data
$mkdir ny_taxi_postgre_data
~~~

### 2. Create Docker network

We will have the following Docker containers:
1. PostgreSQL db
2. pgAdmin
3. Python ingest script

To make them all communicate with each other, we create a network that all containers will connect to.
~~~
$docker create network pg-network
~~~

### 3. Run Docker PostgreSQL

We first pull the official postgres Docker image
~~~
$docker pull postgres
~~~
We run the postgres image specifying
- username
- password
- database name
- linking local postgres data dir to container
- port (5432 is the default)
- the docker network we created earlier
- name (we need this to connect with pgAdmin later)
- postgre version
~~~
$docker run -it \
	-e POSTGRE_USER='root' \
	-e POSTGRE_PASSWORD='root' \
	-e POSTGRE_DB='ny_taxi' \
	-v $(pwd)/ny_taxi_postgre_data:var/lib/postgresql/data \
	-p 5432:5432 \
	--network=pg-network \
	--name pg-database
	postgres:13
~~~

### 4. Connecting to Docker PostgreSQL

We can quick test connection with pgcli. Make sure to have it installed in venv.
~~~
$pip install pgcli psycopg[binary,pool]
~~~
Connect to Docker postgre
~~~
$pgcli -h localhost -p 5432 -u root -d ny_taxi_v2
~~~
Test some queries and pgcli commands
~~~
\dt #describe table, should be empty
~~~

If pgcli connected, it means postgre is live in Docker. We'll connect to it with pgAdmin
~~~
docker pull dpage/pgadmin4
~~~
Run pgAdmin specifying
- login email
- password
- port
- shared docker network
- name (optional)
- image
~~~
$docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network-v2 \
  --name pgadmin-docker-v2 \
  dpage/pgadmin4
~~~
Once ran, go to localhost:8080 to access pgAdmin. Login and create new server
- general/name: w.e.
- connections/db: pg-database
- connections/user: username from postgre param (root)
- connections/password: password from postgre param (root)

### (Optional) Use Docker compose to run Postgres and pgAdmin

If params are likely to be static, then we can use `docker-compose.yaml` or `compose.yaml` in project directory to run it with one simple command. First we translate to compose file:
~~~
#compose.yaml file
services:
	pgdatabase:
		image: postgres:13
		environment:
			- POSTGRES_USER=root
			- POSTGRES_PASSWORD=root
			- POSTGRES_DB=ny_taxi
		volumes:
			- .ny_taxi_postgre_data:var/lib/postgresql/data:rw
		ports:
			- "5432:5432"
	pgadmin:
		image:dpage/pgadmin4
	    environment:
	      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
	      - PGADMIN_DEFAULT_PASSWORD=root
	    ports:
	      - "8080:80"
~~~

Notice we didn't specify network, Docker compose will do it for us if we run two containers within one file.

~~~
$docker compose up
~~~

### 5. Ingest data into PostgreSQL

We have postgre and pgAdmin up and running in one Docker network, but we don't have data yet. We ingest data with a Python script `ingest_data.py` in project folder which does the following:
- transform parquet data into df with `pyarrow`
- connect to postgresql with `sqlalchemy`
- write chunks to db (its over 1m rows)
- set command line params with `argparse`

~~~
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
	engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

	#write in chunks to db
	n=100000
	trips_chunks = [trips[i:i+n] for i in range(0, trips.shape[0], n)]

	for n in range(0, len(trips_chunks)):
		trips_chunks[n].to_sql(name=table, con=engine, if_exists='append')
		print(f'chunk {n+1}/{len(trips_chunks)+1} complete')

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Ingest data script')
	parser.add_argument('--username', help='username for postgres')
	parser.add_argument('--password', help='password for postgres')
	parser.add_argument('--host', help='host for docker, usually localhost')
	parser.add_argument('--port', help='port for docker postgres, usually 5432')
	parser.add_argument('--database', help='postgres database name')
	parser.add_argument('--table', help='postgres table name')

	args = parser.parse_args()
	main(args)
~~~

We can run this script from the terminal
~~~
$python ingest_data.py \
	--username=root \
	--password=root \
	--host=localhost \
	--port=5432 \
	--database=ny_taxi \
	--table=yellow_taxi
~~~

#### Optional: Dockerize the injest script

We want to Dockerize this ingestion script to have it consistent and shareable and also to set up additional workflows in the future.

In project folder, we create a file called `Dockerfile` without any extensions that will tell Docker to:
1. Install python 3.11
2. Pip install required libraries
3. Set working directory within container
4. Copy relevant files and folders into container
5. What to execute after running
~~~
FROM python:3.11
RUN pip install pandas pyarrow sqlalchemy psycopg2
WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY ./ny_taxi_local_data ./ny_taxi_local_data
ENTRYPOINT ["python","ingest_data.py"]
~~~
We build the image from project dir
~~~
$docker build -t taxi_ingest:v1 .
~~~
We run it with the same params as python script with addition of specifying the Docker network to connect to
~~~
$docker run -it \
	--network=pg-network \
	taxi_ingest:v1 \
		--username=root \
		--password=root \
		--host=pg-database \
		--port=5432 \
		--database=ny_taxi \
		--table=yellow_taxi
~~~
With either method to ingest data, python or Docker run, we can now use SQL to query the dataset from either pgAdmin (localhost:8080) or pgcli.
