Cassandra   
docker run --name cassandra -p 9042:9042 -d cassandra:latest

docker exec -it cassandra bash

cqlsh

CREATE KEYSPACE cassandra_tutorial WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE cassandra_tutorial;

    CREATE TABLE log_data (
        interval double,
        date_time_ date,
        ip text,
        url text,
        status int,
        size int,
        duration double,
        PRIMARY KEY (ip)
        );
