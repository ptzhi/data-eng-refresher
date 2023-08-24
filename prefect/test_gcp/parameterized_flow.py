from pathlib import Path

import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

#do we really need a separate fn for read_csv
@task(log_prints=True, retries=3)
def fetch(url: str) -> pd.DataFrame:
	df = pd.read_csv(url)
	print(f'---LOG: read data complete')
	return df

#do these explorations in jupyter first
@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
	df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
	df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
	print('---LOG: cleaning data complete')
	print(df.head(2))
	print(f'datatypes: {df.dtypes}')
	print(f'dimensions: {df.shape}')
	return df

#write df as parquet for compression
@task(log_prints=True)
def write_parquet(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
	path = Path(f'local_clean_data/{color}/{dataset_file}.parquet')
	df.to_parquet(path, compression='gzip')
	print('---LOG: compress and write to parquet complete')
	return path

#upload to gcs bucket
@task(log_prints=True)
def write_gcs(path: Path) -> None:
	gcp_credentials_block = GcpCredentials.load("gcp-credentials")
	gcs_bucket_block = GcsBucket.load("gcs-bucket")
	print('---LOG: uploading to GCS...')
	gcs_bucket_block.upload_from_path(
		from_path=path,
		to_path=path,
	)
	print('---LOG: uploading to GCS complete')


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
	dataset_file = f'{color}_tripdata_{year}-{month:02}'
	dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
	# dataset_url = f'local_raw_data/{color}/{dataset_file}.csv.gz'
	df_raw = fetch(dataset_url)
	df_clean = clean(df_raw)
	path = write_parquet(df_clean, color, dataset_file)
	write_gcs(path)

@flow()
def etl_parent_flow(
	color: str, year: int, months: list[int]):
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == '__main__':
	color='yellow'
	year=2021
	months=[1,2,3]
	etl_parent_flow(color, year, months)
