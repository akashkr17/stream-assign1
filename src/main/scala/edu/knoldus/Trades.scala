package edu.knoldus

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Trades(trades: List[Trade])
case class Trade(symbol: String, price: Double)
case class FileInfo(fileLocation: String, timestamp: Long)

object TradesFormat extends DefaultJsonProtocol {
  implicit val tradeFormat: RootJsonFormat[Trade] = jsonFormat2(Trade)
  implicit val tradesFormat: RootJsonFormat[Trades] = jsonFormat1(Trades)
  implicit val fileInfoFormat: RootJsonFormat[FileInfo] = jsonFormat2(FileInfo)
}
