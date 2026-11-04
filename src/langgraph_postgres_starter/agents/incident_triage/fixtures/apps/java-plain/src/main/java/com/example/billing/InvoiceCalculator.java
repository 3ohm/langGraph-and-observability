package com.example.billing;

/** Prices a month of seats, prorated by the days each seat was active. */
public final class InvoiceCalculator {

    private static final int DAYS_IN_PERIOD = 30;

    private final long seatPriceCents;

    public InvoiceCalculator(long seatPriceCents) {
        this.seatPriceCents = seatPriceCents;
    }

    public Invoice invoice(Account account) {
        long perSeat = perSeatCents(account);
        return new Invoice(account.id(), account.seats(), perSeat, perSeat * account.seats());
    }

    long perSeatCents(Account account) {
        long discountPercent = Math.min(30, account.seats() / 10);
        long discounted = seatPriceCents * (100 - discountPercent) / 100;
        return discounted * account.activeSeatDays() / ((long) account.seats() * DAYS_IN_PERIOD);
    }
}
