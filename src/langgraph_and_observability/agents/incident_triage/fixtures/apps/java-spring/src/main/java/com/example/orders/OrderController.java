package com.example.orders;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @GetMapping("/orders/{id}")
    public Order get(@PathVariable long id) {
        return orders.find(id);
    }

    @GetMapping("/orders/{id}/label")
    public String label(@PathVariable long id) {
        return orders.shippingLabel(orders.find(id));
    }
}
