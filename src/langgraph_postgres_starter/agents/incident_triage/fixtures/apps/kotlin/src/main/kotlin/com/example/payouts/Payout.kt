package com.example.payouts

import java.math.BigDecimal
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.Connection

enum class PayoutStatus { PENDING_REVIEW, APPROVED, RETRY_SCHEDULED, SENT }

data class Payout(
    val id: String,
    val merchantId: String,
    val amount: BigDecimal,
    val currency: String,
    val status: PayoutStatus,
)

class PayoutRepository(private val db: Connection) {
    /** Approved payouts, and failed ones whose retry is due. */
    fun due(): List<Payout> =
        db.prepareStatement(
            "select id, merchant_id, amount, currency, status from payouts " +
                "where status in ('APPROVED', 'RETRY_SCHEDULED') and due_at <= now() order by due_at",
        ).use { statement ->
            statement.executeQuery().use { rows ->
                buildList {
                    while (rows.next()) {
                        add(
                            Payout(
                                rows.getString("id"),
                                rows.getString("merchant_id"),
                                rows.getBigDecimal("amount"),
                                rows.getString("currency"),
                                PayoutStatus.valueOf(rows.getString("status")),
                            ),
                        )
                    }
                }
            }
        }

    fun markSent(id: String) {
        db.prepareStatement("update payouts set status = 'SENT', sent_at = now() where id = ?").use { statement ->
            statement.setString(1, id)
            statement.executeUpdate()
        }
    }
}

class BankClient(private val baseUrl: String) {
    private val http = HttpClient.newHttpClient()

    fun transfer(merchantId: String, amount: BigDecimal, currency: String) {
        val body = """{"merchant_id":"$merchantId","amount":"$amount","currency":"$currency"}"""
        val request = HttpRequest.newBuilder(URI.create("$baseUrl/transfers"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        check(response.statusCode() == 201) { "bank rejected the transfer: ${response.statusCode()}" }
    }
}
