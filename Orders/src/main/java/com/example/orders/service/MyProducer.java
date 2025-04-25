package com.example.orders.service;

import com.example.orders.model.Order;
import com.example.orders.repository.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MyProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OrderRepository myObjectRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public MyProducer(KafkaTemplate<String, String> kafkaTemplate, OrderRepository myObjectRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.myObjectRepository = myObjectRepository;
    }

    public void sendOrder(Order order) throws JsonProcessingException {

        System.out.println("OrderService take order - " + order.getName() + " " + order.getCount());

        Order saveOrder = myObjectRepository.save(order);

        System.out.println("OrderSeervice save object - " + saveOrder.getName() + " " + saveOrder.getCount());

        String jsonObject = objectMapper.writeValueAsString(saveOrder);

        System.out.println("OrderService send object - " + jsonObject);

        kafkaTemplate.send("new_orders", jsonObject);

    }


    public List<Order> findAll() {
        return myObjectRepository.findAll();
    }
}
