package com.netflix.ads.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topicName;

    public KafkaEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void emitOperationalEvent(String key, String jsonPayload) {
        kafkaTemplate.send(topicName, key, jsonPayload);
        System.out.println("[KafkaProducer] Emitted event to topic (" + topicName + "): " + jsonPayload);
    }
}
