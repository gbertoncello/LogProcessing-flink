Log Processing

Log Processing analisa arquivos de log de servidor HTTP e retorna hosts, número de requests e total de bytes enviados aos respectivos hosts.

Pré-requisitos

- Maven
- Apache Flink 1.2
- Conferir o caminho do arquivo log.csv e alterar conforme o mesmo nos fontes LogProcessing.java nos diretórios batch e stream

Para executar o batch:

1)Confira se o Flink está rodando, caso contrário:

(flink-1.2.0 diretório)/bin/start-local.sh

2)No diretório LogProcessing-flink, crie o arquivo jar usando o Maven:

mvn package

3)Submeta o batch job ao flink

(flink-1.2.0 diretório)/bin/flink run -c batch.LogProcessing ./target/log-processing-0.1.jar

Para executar o stream:

Passos 1 e 2 iguais aos do batch.

3)Iniciar servidor socket:

Dentro do diretório LogProcessing-flink, execute:

java -cp /target/log-processing-0.1.jar stream.DataSetServer (Caminho do arquivo log)/log.csv

4) Em novo terminal, submeta o stream job ao flink:

Dentro do diretório LogProcessing-flink, execute (nesse caso argumentos para flink local):

(flink-1.2.0 diretório)/bin/flink run -c stream.LogProcessing ./target/log-processing-0.1.jar --hostname localhost --port 9999




