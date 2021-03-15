package com.techienotes.services;

import com.google.common.eventbus.EventBus;
import com.techienotes.events.KafkaEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamConsumerService.class);
    private final EventBus eventBus;

    public KafkaStreamConsumerService() {
        eventBus = new EventBus();
    }

    public void subscribe(SubscriptionService service) {
        eventBus.register(service);
    }

    public void unSubscribe(SubscriptionService service) {
        eventBus.unregister(service);
    }

    public void init(String topicName, String groupName, String brokerConfig) {
        try {
            logger.info(String.format("Consuming entries from Kafka topic: %s and GroupId: %s", topicName, groupName));
            Properties properties = new Properties();
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, groupName);
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConfig);
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
            properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
            properties.put("auto.offset.reset", "earliest");
            properties.put("heartbeat.interval.ms", "2000");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//            If kerberos security is enabled
//            properties.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//            properties.put("sasl.kerberos.service.name", "kafka");
//            properties.put("sasl.mechanism", "GSSAPI");
//            System.setProperty("java.security.auth.login.config", "PATH_OF_JAAS");

            final StreamsBuilder builder = new StreamsBuilder();

            KStream<String, byte[]> stream = builder.stream(topicName);
            stream.foreach(new ForeachAction<String, byte[]>() {
                @Override
                public void apply(String key, byte[] value) {
                    eventBus.post(new KafkaEvent(key, value));
                }
            });

            final Topology topology = builder.build();
            final KafkaStreams streams = new KafkaStreams(topology, properties);
            final CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    logger.info("Graceful Shutdown");
                    streams.close();
                    latch.countDown();
                }
            });

            try {
                streams.start();
                logger.info("Streaming started");
                latch.await();
                logger.info("Latch Await Completed");
                logger.info("Streaming Completed");
            } catch (Throwable throwable) {
                System.exit(1);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            logger.error("Exception occured while starting KafkaStreams");
        }
    }
}
