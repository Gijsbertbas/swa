# SWA

## Unpacking AWS DynamoDB backup data
See scripts/unpack_dynamodb_backup.py.

Used to upack DynamoDB backup files in the SWA bucket. The script was executed on an EC2 instance (t3.large) with enough storage to unpack the backup files (±1 Tb).
* copied all files to the EC2 instance
* ran the script
* copied the parsed files to the final destination

## Unpack Google Cloud Storage backup data [OUTDATED]
See scripts/unpack_gcs_backup.py.

Using Rclone to perform the sync. The Rclone config uses env variable for the AWS authentication and a token obtained via the browser for the Google Cloud authentication. To use this token on the EC2 instance, the token was copied there. 
* install Rclone: `sudo -v ; curl https://rclone.org/install.sh | sudo bash`
* copy env file and rclone config to the EC2 instance (`rclone config file` to see the location)
* run `rclone sync google:inactive-usage-data/ gcs/ --progress --include 'daily_usage*'` (or 'p4_hour_data_2024*') to download the files
* run the script
* copy the unpacked files to the final destination `rclone copy extracted/p4_hour_data/ s3://slimwonen-analysis-data/gcs/p4_hour_2024/ --progress`

Some statistics:
* 'hourly_usage_data' is currently 5267 files / 16 Gb, sync to t2.large with 1Tb took 20 mins, parsing ±10 and uploading to S3 4mins.
* 'daily_usage_data' is currently 2737 files / 8 Gb, sync took 5 mins, parsing ±5 and uploading to S3 2min.
* 'p4_day_data' is currently 906 files / 8.6 Gb, sync took 10 mins, parsing ±5 and uploading to S3 3min.
* 'p4_hour_data' for 2024 is 40k files / 390 Gb, sync took 8 hours, parsing 3:40 hours and uploading 2 hours.

## Extract data to SlimWonen AWS account
These steps are taken (and may be repeated) to extract data to the AWS account for analysis.
* get fresh extract of households, merged between several Supabase tables and enriched with lat/lon coordinates:
  * extract using `python scripts/run_extraction.py`
  * replace existing file in S3
* update daily usage data: combine cold storage (until 40 days ago) with recent data
  * from Google Cloud: follow above unpacking flow to download and parse all cold storage files
  * extract from Supabase using `python scripts/run_extraction.py`
  * copy to S3 `rclone copy extracted/daily_usage_data/ s3:slimwonen-analysis-data/gcs/daily_usage/ --progress`

## Run quarter analysis
Kartaalanalyses may be run following the notebook `notebooks/kwartaal_midden_drenthe.ipynb`. This makes use of some helper classes (see [analysis/](./analysis/)) to collect, analyse and plot the data. The data is obtained from AWS Athena.
