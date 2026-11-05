package com.example.payouts

import java.sql.DriverManager
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger("com.example.payouts.Main")

fun main() = runBlocking {
    val repository = PayoutRepository(DriverManager.getConnection(System.getenv("PAYOUTS_DB_URL")))
    val scheduler = PayoutScheduler(repository, BankClient(System.getenv("BANK_API_URL")))
    log.info("payout scheduler started")
    while (true) {
        scheduler.runOnce()
        delay(60_000)
    }
}
