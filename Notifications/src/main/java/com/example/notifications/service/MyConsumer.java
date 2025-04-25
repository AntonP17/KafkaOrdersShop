package com.example.notifications.service;


import com.example.notifications.model.SendOrder;

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

    @KafkaListener(topics = "sent_orders", groupId = "notificationsGroup", containerFactory = "notificationsGroupKafkaListenerContainerFactory")
    public void listenPayedOrder(String jsonMessage) throws InterruptedException, JsonProcessingException {

        System.out.println("Notifications listen order by shipped_orders  " + jsonMessage);

        SendOrder order = objectMapper.readValue(jsonMessage, SendOrder.class);

        System.out.println("Notifications service maping object " + order.getName() + " " + order.getCount());

        Thread.sleep(5000);

        order.setName("Send");

        System.out.println("Notifications service change object " + order.getName() + " " + order.getCount());

        String json = objectMapper.writeValueAsString(order);

        kafkaTemplate.send("products_status", json);

    }

}
