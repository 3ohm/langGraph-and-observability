package com.example.orders;

import java.sql.ResultSet;
import java.sql.SQLException;

public record Order(long id, String customerName, String status, Address shippingAddress) {

    static final String SELECT =
            "select o.id, o.customer_name, o.status, o.ship_line1, o.ship_postcode from orders o";

    static Order fromRow(ResultSet rs, int rowNum) throws SQLException {
        // Click-and-collect orders have no shipping address.
        String postcode = rs.getString("ship_postcode");
        Address address = postcode == null ? null : new Address(rs.getString("ship_line1"), postcode);
        return new Order(rs.getLong("id"), rs.getString("customer_name"), rs.getString("status"), address);
    }
}

record Address(String line1, String postcode) {}
