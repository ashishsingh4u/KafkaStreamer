package com.techienotes;

import com.techienotes.events.KafkaEvent;
import com.techienotes.services.KafkaStreamConsumerService;
import com.techienotes.services.SubscriptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerStreamRunner {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerStreamRunner.class);

    public static void main(String[] args) {
        String topicName = "test-topic";
        String groupName = "stream-runner-group1";
        String brokerConfig = "192.168.86.140:51819";

        KafkaStreamConsumerService service = new KafkaStreamConsumerService();
        service.subscribe(new SubscriptionService() {
            @Override
            public void eventProcessor(KafkaEvent kafkaEvent) {
                logger.info(String.format("Key: %s, Value: %s", kafkaEvent.getKey(), kafkaEvent.getValue()));
            }
        });

        service.init(topicName, groupName, brokerConfig);
    }
}
