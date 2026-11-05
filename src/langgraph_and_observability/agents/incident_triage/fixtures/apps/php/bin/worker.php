<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use App\Pricing\DiscountCalculator;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$connection = new AMQPStreamConnection(getenv('AMQP_HOST'), 5672, getenv('AMQP_USER'), getenv('AMQP_PASS'));
$channel = $connection->channel();
$channel->queue_declare('orders.priced', false, true, false, false);
$calculator = new DiscountCalculator();

$channel->basic_consume('orders.priced', '', false, false, false, false, function (AMQPMessage $message) use ($calculator): void {
    $order = json_decode($message->getBody(), true, flags: JSON_THROW_ON_ERROR);
    $discounted = $calculator->apply($order);
    printf("[%s] worker.INFO: discounted order %s by %d cents\n", date(DATE_ATOM), $discounted['id'], $discounted['discount_cents']);
    $message->ack();
});

printf("[%s] worker.INFO: consuming orders.priced\n", date(DATE_ATOM));
while ($channel->is_consuming()) {
    $channel->wait();
}
