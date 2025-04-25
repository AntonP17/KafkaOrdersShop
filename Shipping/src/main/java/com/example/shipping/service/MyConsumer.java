package com.example.shipping.service;


import com.example.shipping.model.ShippedOrder;
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

    @KafkaListener(topics = "payed_orders", groupId = "shippingGroup", containerFactory = "shippingGroupKafkaListenerContainerFactory")
    public void listenPayedOrder(String jsonMessage) throws InterruptedException, JsonProcessingException {

        System.out.println("ShippingService listen order by payed_orders  " + jsonMessage);

        ShippedOrder order = objectMapper.readValue(jsonMessage, ShippedOrder.class);

        System.out.println("Shipping service maping object " + order.getName() + " " + order.getCount());

        Thread.sleep(5000);

        order.setName("Shipped");

        System.out.println("Shipping service change object " + order.getName() + " " + order.getCount());

        String json = objectMapper.writeValueAsString(order);

        kafkaTemplate.send("sent_orders", json); // новый топик
        kafkaTemplate.send("products_status", json);

    }

}
