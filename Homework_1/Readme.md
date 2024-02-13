# Homework 1

You can find here my answers for the homework_1 ([DEZoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/))

## Question 1 : Which tag has the following text? - *Automatically remove the container when it exits*

Here, is the output of `docker run --help`:

<p align="center">
<img src="images/Output_docker_run.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

## Question 2 : Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use `pip list`). What is version of the package *wheel* ?


<p align="center">
<img src="images/output_python_wheel.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

## Question 3 : How many taxi trips were totally made on September 18th 2019?

After ingesting the data through the following code

``` python
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

python scripts/ingest_param_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=taxi_trips_2019_09 \
  --url=${URL} 


URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

python ingest_param_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url=${URL}  
```
we get the result through  the following request

<p align="center">
<img src="images/Output_Question_3.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

## Question 4 : Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

we get the result through  the following request
<p align="center">
<img src="images/Output_question_4.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

## Question 5: Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown. Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

we get the result through  the following request
<p align="center">
<img src="images/Output_question_5.png "
alt="docker run --help" style="width:100%; border:0;">
</p>


## Question 6: For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

we get the result through  the following request

<p align="center">
<img src="images/Output_question_6.png "
alt="docker run --help" style="width:100%; border:0;">
</p>

## Question 7
After doing all the necessary configurations, the output of `terraform apply` is:
```
Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demo_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + max_time_travel_hours      = (known after apply)
      + project                    = "dte-course"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "demo-terra-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/dte-course/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 2s [id=demo-terra-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```
