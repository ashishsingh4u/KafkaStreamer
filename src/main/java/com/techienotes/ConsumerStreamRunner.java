package com.techienotes;

import com.google.protobuf.InvalidProtocolBufferException;
import com.techienotes.services.KafkaStreamConsumerService;
import com.techinotes.protobuf.Technology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerStreamRunner {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerStreamRunner.class);

    public static void main(String[] args) {
        String topicName = "tech-topic";
        String groupName = "stream-runner-group1";
        String brokerConfig = "192.168.86.140:51351";

        KafkaStreamConsumerService service = new KafkaStreamConsumerService();
        service.subscribe(kafkaEvent -> {
            try {
                Technology technology = Technology.parseFrom(kafkaEvent.getValue());
                logger.info(String.format("Key: %s, Value: %s", kafkaEvent.getKey(), technology.toString()));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Exception occured while converting proto byte to object", e);
            }
        });

        service.init(topicName, groupName, brokerConfig, false);
    }
}
