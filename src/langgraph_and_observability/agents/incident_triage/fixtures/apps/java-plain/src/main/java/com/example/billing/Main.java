package com.example.billing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Monthly invoices: reads the period's accounts and prints one invoice per account. */
public final class Main {

    private static final long MONTHLY_SEAT_PRICE_CENTS = 4_900;

    public static void main(String[] args) throws Exception {
        Path accountsFile = Path.of(args.length > 0 ? args[0] : "/data/accounts.csv");
        List<Account> accounts = Files.readAllLines(accountsFile).stream().skip(1).map(Account::parse).toList();
        InvoiceCalculator calculator = new InvoiceCalculator(MONTHLY_SEAT_PRICE_CENTS);
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            List<Future<Invoice>> invoices = new ArrayList<>();
            for (Account account : accounts) {
                invoices.add(pool.submit(() -> calculator.invoice(account)));
            }
            for (Future<Invoice> invoice : invoices) {
                System.out.println(invoice.get());
            }
        }
    }
}
