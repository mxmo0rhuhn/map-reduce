#!/bin/bash
java -Djava.util.logging.config.file=/home/rethab/ptemp/map-reduce/mapreduce-shared/src/main/resources/logging.properties -jar target/mapreduce-client-socket-0.9-SNAPSHOT-jar-with-dependencies.jar localhost 4753 10
