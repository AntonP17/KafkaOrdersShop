package com.example.orders.service;


import com.example.orders.model.Order;
import com.example.orders.repository.OrderRepository;
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

    private static final Logger log = LoggerFactory.getLogger(MyConsumer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrderRepository myObjectRepository;

    @Autowired
    public MyConsumer(KafkaTemplate<String, String> kafkaTemplate, OrderRepository myObjectRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.myObjectRepository = myObjectRepository;
    }

    @KafkaListener(topics = "products_status", groupId = "statusGroup", containerFactory = "statusGroupKafkaListenerContainerFactory")
    public void updateStatus(String jsonMessage) throws JsonProcessingException {

        log.info("OrderService listen changed object by product_status  {}", jsonMessage);

        Order order = objectMapper.readValue(jsonMessage, Order.class);

        System.out.println("OrderService maping object " + order.getName() + " " + order.getCount());

        myObjectRepository.save(order);

        System.out.println("Object updated in DB");
    }

}
