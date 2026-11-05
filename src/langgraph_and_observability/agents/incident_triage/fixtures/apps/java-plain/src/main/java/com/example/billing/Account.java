package com.example.billing;

/** An account at the end of the billing period: its seats, and the days they were active. */
public record Account(String id, int seats, long activeSeatDays) {

    /** Parses one line of accounts.csv: {@code id,seats,active_seat_days}. */
    static Account parse(String line) {
        String[] fields = line.split(",");
        return new Account(fields[0], Integer.parseInt(fields[1]), Long.parseLong(fields[2]));
    }
}

record Invoice(String accountId, int seats, long perSeatCents, long totalCents) {}
