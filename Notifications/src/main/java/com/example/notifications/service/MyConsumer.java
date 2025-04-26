package com.example.notifications.service;


import com.example.notifications.model.SendOrder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MyConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Logger log = LoggerFactory.getLogger(MyConsumer.class);

    @Autowired
    public MyConsumer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;

    }

    @KafkaListener(topics = "sent_orders", groupId = "notificationsGroup", containerFactory = "notificationsGroupKafkaListenerContainerFactory")
    public void listenPayedOrder(String jsonMessage)  {

        try {
            log.info("Processing notifications for order: {}", jsonMessage);

            SendOrder order = objectMapper.readValue(jsonMessage, SendOrder.class);
            log.debug("Mapped order: {} (count: {})", order.getName(), order.getCount());

          //  processNotificationsAsync(order);

            order.setName("Sent");

            String sentJson = objectMapper.writeValueAsString(order);

            kafkaTemplate.send("products_status", sentJson);

            log.info("NotificatioN processed for order ID: {}", order.getId());

        } catch (JsonProcessingException e) {
            log.error("Failed to parse order: {}", jsonMessage, e);
            throw new RuntimeException("JSON processing failed", e);
        }

    }

    private void processNotificationsAsync(SendOrder order) {

        try {
            log.debug("Simulating notification delay for order: {}", order.getId());
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Notification processing interrupted", e);
        }
    }

}
