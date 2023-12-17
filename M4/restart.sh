#!/bin/bash

# This script is used to restart the M4 service

mkdir -p ./data ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

rm -rf ./dags/__pycache__

docker-compose down
#rm -f ./data/gps_locations.csv
rm -f ./data/green_tripdata_2018-11_clean.csv
rm -f ./data/green_tripdata_2018-11_transformed.csv
rm -f ./data/lookup_green_taxi_11_2018.csv

docker volume rm m4_airflow-pgsql-db-volume
docker volume rm m4_green_taxi-pgsql-db-volume

docker-compose build

docker-compose up airflow-init
docker-compose up -d