package com.techienotes.services;

import com.google.common.eventbus.Subscribe;
import com.techienotes.events.KafkaEvent;

public interface SubscriptionService {
    @Subscribe
    void eventProcessor(KafkaEvent kafkaEvent);
}
