<?php

declare(strict_types=1);

namespace App\Pricing;

final class DiscountCalculator
{
    /**
     * Spreads the order's discount evenly over the lines that are eligible for it.
     *
     * @param array{id: string, discount_cents: int, lines: list<array{sku: string, price_cents: int, eligible: bool}>} $order
     */
    public function apply(array $order): array
    {
        $eligible = [];
        foreach ($order['lines'] as $index => $line) {
            if ($line['eligible']) {
                $eligible[] = $index;
            }
        }
        $share = intdiv($order['discount_cents'], count($eligible));
        foreach ($eligible as $index) {
            $order['lines'][$index]['price_cents'] -= $share;
        }
        return $order;
    }
}
