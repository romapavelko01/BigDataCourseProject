#!/bin/bash

docker network create pavdobr-project-network
docker run --name cassandra-node --network pavdobr-project-network -p 9042:9042 -d cassandra:latest
sleep 65s

docker build -t ddl_image -f DockerfileDDL .
docker run -it --network pavdobr-project-network --rm ddl_image