package com.example.shipping

final case class Parcel(id: String, carrier: String, weightGrams: Int, zone: Int)

object Parcel {
  /** Parses one line of a parcel file: id,carrier,weight_grams,zone. */
  def parse(line: String): Parcel = {
    val fields = line.split(',')
    Parcel(fields(0), fields(1), fields(2).toInt, fields(3).toInt)
  }
}

final case class Quote(parcelId: String, carrier: String, priceCents: Long)

class RateCalculator(surchargePerKgCents: Long) {

  def quoteAll(parcels: List[Parcel]): List[Quote] =
    parcels.map(quote)

  def quote(parcel: Parcel): Quote = {
    val weightKg = math.ceil(parcel.weightGrams / 1000.0).toLong
    val price = baseRateCents(parcel.carrier, parcel.zone) + weightKg * surchargePerKgCents
    Quote(parcel.id, parcel.carrier, price)
  }

  def baseRateCents(carrier: String, zone: Int): Long =
    (carrier, zone) match {
      case ("royal_mail", 1) => 385
      case ("royal_mail", _) => 690
      case ("dpd", 1)        => 520
      case ("dpd", _)        => 840
    }
}
