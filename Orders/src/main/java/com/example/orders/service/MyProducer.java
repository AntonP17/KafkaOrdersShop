package com.example.orders.service;

import com.example.orders.model.Order;
import com.example.orders.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MyProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OrderRepository myObjectRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Logger log = LoggerFactory.getLogger(MyProducer.class);

    @Autowired
    public MyProducer(KafkaTemplate<String, String> kafkaTemplate, OrderRepository myObjectRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.myObjectRepository = myObjectRepository;
    }

    public void sendOrder(Order order) {

        log.info("Order received: {} (count: {})", order.getName(), order.getCount());

        Order saveOrder = myObjectRepository.save(order);
        log.debug("Order saved: ID={}", saveOrder.getId());

        try {
            String jsonObject = objectMapper.writeValueAsString(saveOrder);
            kafkaTemplate.send("new_orders", jsonObject);
            log.info("Order sent to Kafka: {}", jsonObject);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize order: {}", e.getMessage());
            throw new RuntimeException("Serialization error", e); // или кастомный exception
        }
    }


    public List<Order> findAll() {
        return myObjectRepository.findAll();
    }
}
