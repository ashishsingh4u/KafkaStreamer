# KafkaStreamer

## Kafka Commands (Perform kinit on the console before running these commands)

### To reset consumer group offset to earliest 

```shell
/usr/hdp/current/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 192.168.86.140:51819 --group stream-runner-group1 --reset-offsets --to-earliest --all-topics --execute --security-protocol SASL_PLAINTEXT
```

### To describe consumer group

```shell
/usr/hdp/current/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 192.168.86.140:51819 --group stream-runner-group1 --describe --security-protocol SASL_PLAINTEXT
```

### To create topics

```shell
export KAFKA_CLIENT_KERBEROS_PARAMS="-Djava.security.auth.login.config=jaas.conf"
/usr/hdp/current/kafka/bin/kafka-topics.sh --zookeeper 192.168.86.140:2181 --create --topic stream-runner --partition 2 --replication-factor 2
```

### To delete topics

```shell
export KAFKA_CLIENT_KERBEROS_PARAMS="-Djava.security.auth.login.config=jaas.conf"
/usr/hdp/current/kafka/bin/kafka-topics.sh --zookeeper 192.168.86.140:2181 --delete --topic stream-runner
```

### To produce messages

```shell
export KAFKA_CLIENT_KERBEROS_PARAMS="-Djava.security.auth.login.config=jaas.conf"
/usr/hdp/current/kafka/bin/kafka-console-producer.sh --broker-list 192.168.86.140:51819 --topic stream-runner --property "parse.key=true" --property "key.separator=:" --security-protocol SASL_PLAINTEXT 
```

### Run producer app

```shell
java -cp target/KafkaStreamer-1.0-SNAPSHOT-shaded.jar com.techinotes.KafkaProducerRunner
```

### Run consumer app

```shell
java -jar target/KafkaStreamer-1.0-SNAPSHOT-shaded.jar
```

