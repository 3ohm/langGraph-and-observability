package com.example.payouts

import org.slf4j.LoggerFactory

/** Sends the payouts that are due to the merchants' bank accounts. */
class PayoutScheduler(private val repository: PayoutRepository, private val bank: BankClient) {
    private val log = LoggerFactory.getLogger(PayoutScheduler::class.java)

    fun runOnce() {
        for (payout in repository.due()) {
            send(payout)
        }
    }

    private fun send(payout: Payout) {
        if (payout.status != PayoutStatus.APPROVED) {
            error("payout ${payout.id} is ${payout.status}, expected APPROVED")
        }
        bank.transfer(payout.merchantId, payout.amount, payout.currency)
        repository.markSent(payout.id)
        log.info("sent payout {} ({} {})", payout.id, payout.amount, payout.currency)
    }
}
