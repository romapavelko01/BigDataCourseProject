#!/bin/bash

docker build -t read_wiki_stream -f DockerfileReadStream .

docker run --name flask-cassandra-api --network pavdobr-project-network -p 8080:8080 -d read_wiki_stream
docker run -it --network pavdobr-project-network --rm read_wiki_stream