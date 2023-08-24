from pathlib import Path

import pandas as pd

from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse

# Copies a folder from the configured GCS bucket to a local directory.
@task(log_prints=True, retries=3)
def extract_from_gcs(color:str, year:int, month:int) -> Path:
	gcs_path=f'local_clean_data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
	# gcs_bucket_block = GcsBucket.load("gcs-bucket")
	# gcs_bucket_block.get_directory(
	# 	from_path=gcs_path,
	# 	local_path=f'gcs_copy_data/'
	# )
	local_path = Path(f'gcs_copy_data/{gcs_path}')
	return local_path

# again just to test, but should be explored in jupyter first
@task(log_prints=True)
def clean_data(path: Path) -> pd.DataFrame:
	df = pd.read_parquet(path)
	print(f'---LOG: pre-clean count of na in passenger_count: {df["passenger_count"].isna().sum()}')
	df['passenger_count'].fillna(0, inplace=True)
	print(f'---LOG: post-clean count of na in passenger_count: {df["passenger_count"].isna().sum()}')
	return df

# creates empty dataset and table
@task(log_prints=True)
def create_bq_table() -> None:
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")
    bigquery_warehouse_block = BigQueryWarehouse.load("bigquery-block")
    client = gcp_credentials_block.get_bigquery_client()
    client.create_dataset("ny_taxi", exists_ok=True)
    client.create_table("ny_taxi.yellow_taxi_2021-01", exists_ok=True)

# writes parquet file to bigquery
@task(log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
	gcp_credentials_block = GcpCredentials.load("gcp-credentials")
	df.to_gbq(
		destination_table='dtc-de-course-396514.ny_taxi.yellow_taxi_2021-01',
		project_id='dtc-de-course-396514',
		credentials=gcp_credentials_block.get_credentials_from_service_account(),
		chunksize=500_000,
		if_exists='replace' #change to append if repeating this job
	)

@flow()
def etl_gcs_to_bq():
	color = "yellow"
	year = 2021
	month = 1
	path = extract_from_gcs(color, year, month)
	df_clean = clean_data(path)
	try:
		write_to_bq(df_clean)
	except:
		create_bq_table()
		write_to_bq(df_clean)


if __name__ == '__main__':
	etl_gcs_to_bq()
