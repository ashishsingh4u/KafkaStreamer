package com.techienotes;

import com.techienotes.services.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaProducerRunner {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerRunner.class);

    public static void main(String[] args) {
        String topicName = "test-topic";
        String brokerConfig = "192.168.86.140:51819";

        new Thread(new KafkaProducerService(topicName, brokerConfig)).start();
        logger.info("Kafka Producer Started");
    }
}
