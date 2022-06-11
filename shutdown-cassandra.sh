docker stop cassandra-node
docker stop flask-cassandra-api
docker rm cassandra-node
docker rm flask-cassandra-api
docker network rm pavdobr-project-network