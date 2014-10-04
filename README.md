Das Projekt map-reduce besteht momentan aus drei Projekten (Maven-Jargon: Module).
        mapreduce-master: Funktionalität für den Server, der als Master agiert
        mapreduce-shared: Interfaces, die auf einem Client/Worker sowie auf dem Master zur Laufzeitexistieren müssen
        mapreduce-client-socket: Dieser Teil muss auf einem Client/Worker, der über einen Socket mit dem Master kommuniziert

Vom Parent-Projekt könnte man theoretisch alle zusammen builden, aber typischerweise geht man in eines der Submodule und
arbeitet dort ganz normal mit Maven. Das Master-Projekt, sowie das Client-Socket-Projekt haben eine Abhängigkeit auf
das Shared-Projekt. Um diese korrekt aufzulösen, muss also das Shared-Projekt lokal in der richtigen Version installiert sein.
Dies kann mit 'mvn install' erledigt werden. Wenn install vom Parent-Projekt ausgeführt wird, werden alle Sub-Projekte
installiert.

Im Parent-Projekt pom.xml gibt es neue eine Sektion dependencyManagement. Dort werden für alle Sub-Projekte die Versionen
der Abhängigkeiten definiert, somit müssen die Sub-Projekte nur noch die Abhängigkeiten selbst angeben. Ausserdem definiert
das Parent-Projekt einige weitere Gemeinsamkeiten, wie zum Beispiel das Encoding, die Target Java-Version, etc.
