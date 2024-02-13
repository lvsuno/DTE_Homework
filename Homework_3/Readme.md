
I use Mage to load the 2022 Green Taxi Trip Record Parquet Files and export it in a GCS bucket.


The Data loader code is the following:

```
import io
import pandas as pd
import requests
import pyarrow.parquet as pq

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-'
    ext = '.parquet'

    months = ['01','02','03','04','05','06','07','08','09','10','11','12']

    df = pd.DataFrame()

    for i in months:
        # load file with pd.concat
        
        df = pd.concat([df, pd.read_parquet(f'{url}{i}{ext}',
        engine= 'pyarrow',
        )], ignore_index=True)
    
    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'lpep_pickup_datetime': 'datetime64[s]',
                    'lpep_dropoff_datetime': 'datetime64[s]',
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'trip_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }
    
    df = df.astype(dtype=taxi_dtypes)



    return df
    

@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'

```

I don't transform the data since in this homework it's not required. The Data Exporter code is:

```
import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dte-course-9956fa8f2170.json"
bucket_name = 'data_warehouse_homework'
project_id = 'dte-course'

table_name = 'green_taxi_2022.parquet'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()


    pq.write_table(
        table,
        root_path,
        filesystem=gcs,
        coerce_timestamps='ms',
        allow_truncated_timestamps=True
    )
```

The SQL code for the exercise can be found in [Homework_3](../scripts/Homework_3/)