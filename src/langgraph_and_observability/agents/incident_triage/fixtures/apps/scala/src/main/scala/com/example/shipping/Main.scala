package com.example.shipping

import java.io.File
import scala.io.Source
import scala.util.Using

/** Quotes the parcels that the packing stations drop in the inbox folder, as CSV files. */
object Main {
  def main(args: Array[String]): Unit = {
    val inbox = new File(sys.env.getOrElse("PARCEL_INBOX", "/data/parcels"))
    val calculator = new RateCalculator(surchargePerKgCents = 45)
    println(s"shipping-rates watching ${inbox.getPath}")
    while (true) {
      val files = Option(inbox.listFiles()).toList.flatten.filter(_.getName.endsWith(".csv"))
      val parcels = files.flatMap(readParcels)
      val quotes = calculator.quoteAll(parcels)
      quotes.foreach(q => println(s"quoted parcel ${q.parcelId} ${q.carrier} ${q.priceCents}"))
      files.foreach(_.delete())
      Thread.sleep(5000)
    }
  }

  def readParcels(file: File): List[Parcel] =
    Using.resource(Source.fromFile(file))(_.getLines().drop(1).map(Parcel.parse).toList)
}
