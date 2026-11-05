# frozen_string_literal: true

module Fulfillment
  # Renders the shipping label of a packed order.
  class LabelPrinter
    CARRIER_PREFIX = { 'royal_mail' => 'RM', 'dpd' => 'DPD', 'evri' => 'EV' }.freeze

    def initialize(printer_url)
      @printer_url = printer_url
    end

    def render(order)
      [
        order.fetch('name'),
        *address_lines(order.fetch('address')),
        "#{CARRIER_PREFIX.fetch(order.fetch('carrier'))} #{order.fetch('tracking_number')}"
      ].join("\n")
    end

    private

    def address_lines(address)
      [address['line1'], address['line2'], "#{address['city']} #{address['postcode'].upcase}"].compact
    end
  end
end
