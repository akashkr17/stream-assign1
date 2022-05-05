package edu.knoldus

import java.nio.file.{Files, Paths}
import java.util.{Locale, UUID}

import spray.json.DefaultJsonProtocol
import spray.json._
import scala.util.Random

object RandomFileGenerator extends App with TradesFormat {
  while (true) {
    val size = Random.between(3, 20)
    val time = Random.between(1, 3)
    println(s"Waiting for second : $time")
    Thread.sleep(time * 1000)
    val tradeList = (1 to size).map { _ =>
      val symbol = Random.nextString(3).toUpperCase(new Locale("en", "IND"))
      val price = BigDecimal(Random.between(10.0, 100.0))
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
      Trade(symbol, price)
    }.toList

    val trades = Trades(tradeList)
    val fileName = UUID.randomUUID().toString
    Files.write(Paths.get(s"src/main/resources/trades-files/$fileName.json"),
                trades.toJson.toString.getBytes())
  }

}
