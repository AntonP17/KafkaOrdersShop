package com.example.payment.service;

import com.example.payment.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MyConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    public MyConsumer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;

    }

    @KafkaListener(topics = "new_orders", groupId = "paymentGroup", containerFactory = "paymentGroupKafkaListenerContainerFactory")
    public void listenOrder(String jsonMessage) throws InterruptedException, JsonProcessingException {

        System.out.println("PaymentService listen order by new_orders  " + jsonMessage);

        Order order = objectMapper.readValue(jsonMessage, Order.class);

        System.out.println("PaymentService maping object " + order.getName() + " " + order.getCount());

        Thread.sleep(5000);

        order.setName("Payed");

        System.out.println("PaymentService change object " + order.getName() + " " + order.getCount());

        String json = objectMapper.writeValueAsString(order);

        kafkaTemplate.send("payed_orders", json); // новый топик
        kafkaTemplate.send("products_status", json);

    }

}
