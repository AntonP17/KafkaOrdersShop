package com.example.orders.controller;


import com.example.orders.model.Order;
import com.example.orders.service.MyProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class OrderController {

    private final MyProducer producer;

    @Autowired
    public OrderController(MyProducer producer) {
        this.producer = producer;

    }

    @PostMapping("/send")
    public void send(@RequestBody Order order) throws JsonProcessingException {

        producer.sendOrder(order);

    }

    @GetMapping("/all")
    public List<Order> getAll() {
        return producer.findAll();
    }
}
