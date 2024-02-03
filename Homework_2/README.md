We create a pipeline named `green_taxi_etl` as we can see in the following image:

<p align="center">
<img src="images/Pipelines_front.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

The code for the data loader block is:

<p align="center">
<img src="images/Data_loader_code.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

The code for the transformer block is:
<p align="center">
<img src="images/Transformer_code.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

The code for the Postgres data explorer block is:
<p align="center">
<img src="images/Postgres_exporter_code.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

The code to write our data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date` is:
<p align="center">
<img src="images/GCP_exporter_code.png "
alt="docker run --help" style="width:100%; border:0;">
</p>