#!/bin/bash

# Startskipt für Socket Clients
#   - Es wird das Logging Config File gesetzt
#   - Mit JAVA -JAR wird die Klasse SocketClientStart gestartet und dieser werden die
#     Argumente für den Master-Server, Master-Port und Anzahl Worker, die dieser Client
#     starten soll, übergeben.

#java -Djava.util.logging.config.file=logging.properties -jar target/mapreduce-client-socket-0.10-SNAPSHOT-jar-with-dependencies.jar 160.85.133.224 4753 20
java -Djava.util.logging.config.file=logging.properties -jar target/mapreduce-client-socket-0.10-SNAPSHOT-jar-with-dependencies.jar localhost 4753 40
