package edu.knoldus.zio

import java.io.{BufferedReader, File, FileInputStream, FileReader, IOException, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.json._
import zio.kafka.consumer._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.random.Random
import zio.stream._

import scala.io.{BufferedSource, Source}

object ZioReaderProducer extends zio.App {

  val filesPath = List("src/main/resources/newData.json",
                       "src/main/resources/newData1.json",
                       "src/main/resources/newData2.json")
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
  val pathSerde: Serde[Any, PathList] = Serde.string.inmapM { string =>
    ZIO.fromEither(
      string
        .fromJson[PathList]
        .left
        .map(errorMessage => new RuntimeException(errorMessage)))
  } { paths =>
    ZIO.effect(paths.toJson)
  }
  val managedProducer = Producer.make(ProducerSettings(List("localhost:29093")),
                                      Serde.string,
                                      pathSerde)
  val producer: ZLayer[Blocking, Throwable, Producer[Any, String, PathList]] =
    ZLayer.fromManaged(managedProducer)

  val record: ProducerRecord[String, PathList] = new ProducerRecord("streamTopic", "key-1", PathList(filesPath))
  val producerEffect
    : ZIO[Producer[Any, String, PathList], Throwable, RecordMetadata] =
    Producer.produce(record)
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    producerEffect.provideSomeLayer(producer).exitCode
}

