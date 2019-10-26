# Proyecto para Práctica de BigData Processing

Por Javier Miguel


## Kafka

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

### Enviar mensajes del fichero

```shell script
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Celebrum > MensajesCapturados.csv
```
