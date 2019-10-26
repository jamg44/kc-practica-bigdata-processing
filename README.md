# Proyecto para Práctica de BigData Processing

Por Javier Miguel

## Preparar entorno local de ejecución

https://kafka.apache.org/quickstart

### Arrancar Zookeeper

```shell script
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Arrancar Kafka

```shell script
bin/kafka-server-start.sh config/server.properties
```

### Crear un topic (si no lo hemos hecho ya)

```shell script
> bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Calebrum
```

Ver lista de topics

```shell script
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Ejecutar el proyecto

Primero debemos tener en ejecución Kafka. Ver [Preparar entorno local de ejecución](#preparar-entorno-local-de-ejecución)

En IntelliJ Idea ejecutar src/main/scala/espias/EspiarMensajes.scala

Tras esto podemos enviar mensajes del fichero:

```shell script
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Celebrum > MensajesCapturados.csv
```
