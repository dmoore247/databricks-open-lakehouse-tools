# Zero Copy Open Lakehouse Architecture

- vdbfs.py: Well governed `cp`, `put`, and access to AWS S3 via the open `temporary-volume-credentials` api
  - Unity Catalog volume as an abstraction over S3 bucket/folder
  - temporary-volume-credentials REST api to gain access to the volume
  - `aws` cli for (parallel) object upload / download / ls
  - BYOL api to record file movement lineage

- uploader.py: Bulk upload a (large) Pandas dataframe into a Databricks table.
  - `pandas.to_parquet()` to create compact representation of the `pandas` dataframe.
  -  Databricks file upload api for easy (large) file upload support.
  -  A volume for secure temp storage,
  -  The MERGE INTO sql statement to merge file contents into a open table
  
  Requires env variables:
  - DATABRICKS_CONFIG_PROFILE
  - DATABRICKS_HTTP_PATH
  - DATABRICKS_SERVER_HOSTNAME
  
  Pre-requisites:
  - Login to Databricks `databricks auth login --profile <DATABRICKS_CONFIG_PROFILE>`
  - `pip install -r uploader-requirements.txt`
