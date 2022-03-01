package edu.knoldus

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Trades(trades: List[Trade])
case class Trade(symbol: String, price: Double)
case class FileInfo(fileLocation: String, timestamp: Long)

case class SameTrade(symbol: String, price: List[Prices])
case class Prices(price: Double, timestamp: Long)
case class CombineTrades(trades: List[SameTrade])

object TradesFormat extends DefaultJsonProtocol {

  implicit val tradeFormat: RootJsonFormat[Trade] = jsonFormat2(Trade)
  implicit val tradesFormat: RootJsonFormat[Trades] = jsonFormat1(Trades)
  implicit val fileInfoFormat: RootJsonFormat[FileInfo] = jsonFormat2(FileInfo)

  implicit val pricesFormat: RootJsonFormat[Prices] = jsonFormat2(Prices)
  implicit val traderFormat: RootJsonFormat[SameTrade] = jsonFormat2(SameTrade)
  implicit val combineTradesFormat: RootJsonFormat[CombineTrades] = jsonFormat1(
    CombineTrades)

}
