#!/bin/bash

docker build -t cassandra_write_stream_image -f DockerfileStreamToCassandra .

docker run -it --name stream-cassandra --network pavdobr-project-network --rm cassandra_write_stream_image