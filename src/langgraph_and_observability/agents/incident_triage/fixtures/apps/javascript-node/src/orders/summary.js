'use strict';

const TAX_RATE = 0.2;

function skuOf(item) {
  return item.sku;
}

function addLine(sum, item) {
  return sum + item.unitPrice * item.quantity;
}

function summarizeOrder(order) {
  const skus = order.items.map(skuOf);
  const subtotal = order.items.reduce(addLine, 0);
  return {
    orderId: order.id,
    customerId: order.customerId,
    skus,
    subtotal: Math.round(subtotal * 100) / 100,
    tax: Math.round(subtotal * TAX_RATE * 100) / 100,
  };
}

module.exports = { summarizeOrder };
