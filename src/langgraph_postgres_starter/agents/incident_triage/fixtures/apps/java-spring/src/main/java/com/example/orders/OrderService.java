package com.example.orders;

import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final JdbcClient jdbc;

    public OrderService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Order find(long id) {
        return jdbc.sql(Order.SELECT + " where o.id = ?").param(id).query(Order::fromRow).single();
    }

    public List<Order> paidNotShipped() {
        return jdbc.sql(Order.SELECT + " where o.status = 'PAID'").query(Order::fromRow).list();
    }

    public void queuePendingShipments() {
        List<String> labels = paidNotShipped().stream().map(this::shippingLabel).toList();
        log.info("Queued {} shipments for orders paid while the service was down", labels.size());
    }

    String shippingLabel(Order order) {
        String postcode = order.shippingAddress().postcode().toUpperCase(Locale.ROOT);
        return order.customerName() + "\n" + order.shippingAddress().line1() + "\n" + postcode;
    }
}
