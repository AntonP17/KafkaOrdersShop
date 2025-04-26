package com.example.shipping.service;


import com.example.shipping.model.ShippedOrder;
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

    @KafkaListener(topics = "payed_orders", groupId = "shippingGroup", containerFactory = "shippingGroupKafkaListenerContainerFactory")
    public void listenPayedOrder(String jsonMessage) {

        try {
            log.info("Processing shipping for order: {}", jsonMessage);

            ShippedOrder order = objectMapper.readValue(jsonMessage, ShippedOrder.class);
            log.debug("Mapped order: {} (count: {})", order.getName(), order.getCount());

           // processShippingAsync(order);

            order.setName("Shipped");

            String shippedJson = objectMapper.writeValueAsString(order);

            kafkaTemplate.send("sent_orders", shippedJson); // новый топик
            kafkaTemplate.send("products_status", shippedJson);

            log.info("Shipping processed for order ID: {}", order.getId());

        } catch (JsonProcessingException e) {
            log.error("Failed to parse order: {}", jsonMessage, e);
            throw new RuntimeException("Shipping processing failed", e);
        }

    }

    private void processShippingAsync(ShippedOrder order) {

        try {
            log.debug("Simulating shipping delay for order: {}", order.getId());
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Shipping processing interrupted", e);
        }
    }

}
