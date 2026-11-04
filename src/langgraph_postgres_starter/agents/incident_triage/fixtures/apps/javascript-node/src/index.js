'use strict';

const Redis = require('ioredis');
const { summarizeOrder } = require('./orders/summary');

const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
const subscriber = new Redis(redisUrl);
const publisher = new Redis(redisUrl);

subscriber.subscribe('orders.created', (err) => {
  if (err) {
    console.error(`subscribe failed: ${err.message}`);
    process.exit(1);
  }
  console.log('cart-events subscribed to orders.created');
});

subscriber.on('message', (channel, message) => {
  const order = JSON.parse(message);
  const summary = summarizeOrder(order);
  publisher.publish('orders.summarized', JSON.stringify(summary));
  console.log(`order ${summary.orderId} summarized: ${summary.skus.length} skus, subtotal ${summary.subtotal}`);
});
