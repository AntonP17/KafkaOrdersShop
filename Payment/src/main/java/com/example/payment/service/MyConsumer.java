package com.example.payment.service;

import com.example.payment.model.PayedOrder;
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

    @KafkaListener(topics = "new_orders", groupId = "paymentGroup", containerFactory = "paymentGroupKafkaListenerContainerFactory")
    public void listenOrder(String jsonMessage)  {


        try {
            log.info("Processing payment for order: {}", jsonMessage);

            PayedOrder order = objectMapper.readValue(jsonMessage, PayedOrder.class);
            log.debug("Mapped order: {} (count: {})", order.getName(), order.getCount());

          //  processPaymentAsync(order);

            order.setName("Payed");

            String paymentJson = objectMapper.writeValueAsString(order);

            kafkaTemplate.send("payed_orders", paymentJson); // новый топик
            kafkaTemplate.send("products_status", paymentJson);

            log.info("Payment processed for order ID: {}", order.getId());

        } catch (JsonProcessingException e) {
            log.error("Failed to parse order: {}", jsonMessage, e);
            throw new RuntimeException("Payment processing failed", e);
        }

    }

    private void processPaymentAsync(PayedOrder order) {

        try {
            log.debug("Simulating payment delay for order: {}", order.getId());
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Payment processing interrupted", e);
        }
    }
}
