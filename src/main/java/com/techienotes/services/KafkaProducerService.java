package com.techienotes.services;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerService implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final String topicName;
    private final KafkaProducer<String, byte[]> kafkaProducer;
    private boolean shouldExit = false;

    public KafkaProducerService(String topicName, String bootstrapCofig) {
        logger.info(String.format("Kafka Producer running in thread %s", Thread.currentThread().getName()));
        this.topicName = topicName;
        Properties kafkaProperties = new Properties();

        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapCofig);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProperties.put("security.protocol", "PLAINTEXT");
//        If Kerberos is enable then use these properties
//        kafkaProperties.put("sasl.kerberos.service.name", "kafka");
//        kafkaProperties.put("sasl.mechanism", "GSSAPI");
//        kafkaProperties.put("security.protocol", "SASL_PLAINTEXT");
//        System.setProperty("java.security.auth.login.config", "PATH_OF_JAAS");

        this.kafkaProducer = new KafkaProducer<String, byte[]>(kafkaProperties);
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
        } catch (Exception exception) {
            logger.error(exception.getMessage(), exception);
        }
    }

    private void produce() {
        ProducerRecord<String, byte[]> record;

        try {
            Random rnd = new Random();
            while (!shouldExit) {
                for (int i = 1; i <= 10; i++) {
                    String key = "machine-" + i;
                    String value = String.valueOf(rnd.nextInt(20));
                    record = new ProducerRecord<>(topicName, key, value.getBytes());

                    kafkaProducer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.warn(String.format("Error sending message with key %s\n%s", key, e.getMessage()));
                            } else {
                                logger.warn(String.format("Partition for key-value %s::%s is %s", key, value, recordMetadata.partition()));
                            }
                        }
                    });

                    Thread.sleep(5000);
                }
            }
        } catch (Exception exception) {
            logger.error("Producer thread was interrupted");
        } finally {
            kafkaProducer.close();
            logger.info("Producer closed");
        }
    }
}
