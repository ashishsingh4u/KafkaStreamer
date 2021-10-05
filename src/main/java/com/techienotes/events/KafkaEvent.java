package com.techienotes.events;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class KafkaEvent {
    private String key;
    private byte[] value;
}
