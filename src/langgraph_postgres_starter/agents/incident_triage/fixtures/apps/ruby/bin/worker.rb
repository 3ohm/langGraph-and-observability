# frozen_string_literal: true

require 'json'
require 'aws-sdk-sqs'
require_relative '../lib/fulfillment/label_printer'

$stdout.sync = true
printer = Fulfillment::LabelPrinter.new(ENV.fetch('LABEL_PRINTER_URL'))
poller = Aws::SQS::QueuePoller.new(ENV.fetch('PACKED_ORDERS_QUEUE_URL'))

puts 'label-printer polling packed orders'
poller.poll do |message|
  order = JSON.parse(message.body)
  label = printer.render(order)
  puts "printed label for order #{order.fetch('id')} (#{label.lines.count} lines)"
end
