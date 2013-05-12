#!/bin/bash

# Startskipt für Socket Clients
#   - Es wird das Logging Config File gesetzt
#   - Mit JAVA -JAR wird die Klasse SocketClientStart gestartet und dieser werden die
#     Argumente für den Master-Server, Master-Port und Anzahl Worker, die dieser Client
#     starten soll, übergeben.

java -Djava.util.logging.config.file=/home/rethab/ptemp/map-reduce/mapreduce-shared/src/main/resources/logging.properties -jar target/mapreduce-client-socket-0.9-SNAPSHOT-jar-with-dependencies.jar localhost 4753 10
