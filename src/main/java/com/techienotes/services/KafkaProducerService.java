package com.techienotes.services;

import com.techinotes.protobuf.Technology;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerService implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final String topicName;
    private final KafkaProducer<String, byte[]> kafkaProducer;
    private boolean shouldExit = false;

    public KafkaProducerService(String topicName, String bootstrapConfig, boolean isKerberosEnabled) {
        logger.info("Kafka Producer running in thread {}", Thread.currentThread().getName());
        this.topicName = topicName;
        Properties kafkaProperties = new Properties();

        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapConfig);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProperties.put("security.protocol", "PLAINTEXT");
        kafkaProperties.put("max.block.ms", "5000");

        if (isKerberosEnabled) {
            kafkaProperties.put("delivery.timeout.ms", "6000");
            kafkaProperties.put("request.timeout.ms", "5000");
            kafkaProperties.put("batch-size", "10");
//          If Kerberos is enabled then use these properties
            kafkaProperties.put("sasl.kerberos.service.name", "kafka");
            kafkaProperties.put("sasl.mechanism", "GSSAPI");
            kafkaProperties.put("security.protocol", "SASL_PLAINTEXT");
            System.setProperty("java.security.auth.login.config", "PATH_OF_JAAS");
        }

        this.kafkaProducer = new KafkaProducer<>(kafkaProperties);
    }

    @Override
    public void run() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    logger.info("Graceful Shutdown");
                    shouldExit = true;
                    kafkaProducer.close();
                }
            });
            produce();
        } catch (InterruptedException exception) {
            logger.error(exception.getMessage(), exception);
            Thread.currentThread().interrupt();
        }
    }

    private void produce() throws InterruptedException {
        ProducerRecord<String, byte[]> kafkaRecord;

        try {
            while (!shouldExit) {
                for (int i = 1; i <= 10; i++) {
                    Technology technology = Technology.newBuilder()
                            .setId(i)
                            .setName("machine-" + i)
                            .setType("messageType")
                            .setLanguage("Java")
                            .setStars(i)
                            .build();

                    double key = technology.getId();
                    byte[] value = technology.toByteArray();
                    kafkaRecord = new ProducerRecord<>(topicName, Double.toString(key), value);

                    kafkaProducer.send(kafkaRecord, (recordMetadata, e) -> {
                        if (e != null) {
                            logger.warn("Error sending message with key-value {}::{}-->{}", key, value, e.getMessage());
                        } else {
                            logger.warn("Partition for key-value {}::{} is {}", key, value, recordMetadata.partition());
                        }
                    });

                    Thread.sleep(5000);
                }
            }
        } finally {
            kafkaProducer.close();
            logger.info("Producer closed");
        }
    }
}
