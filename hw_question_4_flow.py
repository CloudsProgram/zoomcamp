from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket #this help with re-useable block

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    #green taxi has a different column name lpep_etc...
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color:str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""

    path = Path(f"data/{color}/{dataset_file}.parquet")#.mkdir(parents=True, exist_ok=True)
    print(f"check point 1: {path}")
    df.to_parquet(path, compression="gzip") # need pyarrow installed to get best compression
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    path = Path(path).as_posix() #deal with prefect's bug on converting forward slash to backward slashs for windows
    print(f"check point 2: {path}")
    gcs_block = GcsBucket.load("zoom-gcs") # zoom-gcs is the block that I named
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)
    return

@flow(log_prints=True)
def etl_web_to_gcs(color:str, year:int, month:int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    print(len(df_clean))
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs(color="green", year=2020, month=11)



