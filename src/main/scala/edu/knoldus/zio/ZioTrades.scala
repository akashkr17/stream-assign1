package edu.knoldus.zio

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

case class PathList(paths: List[String])
case class ZioTrades(trades: List[ZioTrade])
case class ZioTrade(symbol: String, price: Double)

object PathList {
  implicit val encoder: JsonEncoder[PathList] =
    DeriveJsonEncoder.gen[PathList]
  implicit val decoder: JsonDecoder[PathList] =
    DeriveJsonDecoder.gen[PathList]
}
object ZioTrades {
  implicit val encoder: JsonEncoder[ZioTrades] =
    DeriveJsonEncoder.gen[ZioTrades]
  implicit val decoder: JsonDecoder[ZioTrades] =
    DeriveJsonDecoder.gen[ZioTrades]
}
object ZioTrade {
  implicit val encoder: JsonEncoder[ZioTrade] =
    DeriveJsonEncoder.gen[ZioTrade]
  implicit val decoder: JsonDecoder[ZioTrade] =
    DeriveJsonDecoder.gen[ZioTrade]
}
