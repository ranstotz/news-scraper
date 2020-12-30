#!/bin/bash

echo -e "\nStarting DynamoDB locally"
java -Djava.library.path=/usr/local/bin/dynamodb/DynamoDBLocal_lib -jar /usr/local/bin/dynamodb/DynamoDBLocal.jar -sharedDb
echo -e "Terminating script\n"

