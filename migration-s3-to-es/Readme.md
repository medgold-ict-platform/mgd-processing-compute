# Readme

This tool is developed in order to work on a EC2 machine that can communicate with the elasticsearch cluster.

## Config file

The only customizable part is the config.json file, in which you can specify only the name of the s3 bucket from which you want to take the documents, and the elasticsearch cluster endpoint.

## Requirements

In order to properly work, this script needs:

* Python3
* External libraries specified in requirements.txt file

## How to use

* `pip3 install -r requirements.txt`
* `python3 migration.py`