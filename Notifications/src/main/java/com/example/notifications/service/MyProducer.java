package com.example.notifications.service;//package com.example.payment.service;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//import java.util.List;
//
//@Service
//public class MyProducer {
//
//    private final KafkaTemplate<String, String> kafkaTemplate;
//    private final ObjectMapper objectMapper = new ObjectMapper();
//
//    @Autowired
//    public MyProducer(KafkaTemplate<String, String> kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//
//    }
//
//    public void sendOrder() throws JsonProcessingException {
//
//        System.out.println("OrderService take order - " + order.getName() + " " + order.getCount());
//
//        Order saveOrder = myObjectRepository.save(order);
//
//        System.out.println("OrderSeervice save object - " + saveOrder.getName() + " " + saveOrder.getCount());
//
//        String jsonObject = objectMapper.writeValueAsString(saveOrder);
//
//        System.out.println("OrderService send object - " + jsonObject);
//
//        kafkaTemplate.send("new_orders", jsonObject);
//
//        kafkaTemplate.send("products_status", jsonObject);
//    }
//
//
//    public List<Order> findAll() {
//        return myObjectRepository.findAll();
//    }
//}
