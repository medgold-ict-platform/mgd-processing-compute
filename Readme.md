# Pre-processing

## Modules

### `athena-query`

It contains the executions script of the Athena queries and a file which stores the output of one of the queries as an example.
The results get uploaded directly into S3.

### `Conversion` 

It contains the script which converts `.nc` and `.grib` files into `.parquet` and into two files, a `.grib` one and a `.parquet` one

### `migration-s3-to-es` 

It contains the scripts that converts and migrates the files stored into S3 to Elasticsearch
