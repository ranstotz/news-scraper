#!/bin/bash

echo -e "\nGetting local table data for RSS_NEWS table"
# get specific tables data (can be a lot)
aws dynamodb scan --table-name RSS_NEWS  --endpoint-url http://localhost:8000
# show tables
# aws dynamodb list-tables --endpoint-url http://localhost:8000
