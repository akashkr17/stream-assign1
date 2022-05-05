package edu.knoldus.zio

import java.io.{File, PrintWriter}

import edu.knoldus.zio.KafkaProducer.filterFileProducer
import zio._
import edu.knoldus.zio.ZioReaderProducer._
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import zio.{Has, Managed, URIO, ZIO, ZLayer, ZManaged}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.random.Random
import zio.stream.ZSink
import zio.json._

import scala.io.{BufferedSource, Source}

object ZioReaderConsumer extends zio.App {


//  val filterFilemanagedProducer: RManaged[Blocking, Producer.Service[Any, String, String]] = Producer.make(ProducerSettings(List("localhost:29093")),
//    Serde.string,
//    Serde.string)
//  val filterFileProducer: ZLayer[Blocking, Throwable, Producer[Any, String, String]] =
//    ZLayer.fromManaged(filterFilemanagedProducer)


  //  val producerEffect
  //  : ZIO[Producer[Any, String, String], Throwable, RecordMetadata] =
  //
  val managedConsumer: RManaged[Clock with Blocking, Consumer.Service] = Consumer.make(
    ConsumerSettings(List("localhost:29093"))
      .withGroupId("zio-group"))
  val consumer: ZLayer[Clock with Blocking with Random, Throwable, Has[Consumer.Service]] = ZLayer.fromManaged(managedConsumer)

  val streams = Consumer
    .subscribeAnd(Subscription.topics("streamTopic"))
    .plainStream(Serde.string, pathSerde)
    .map { cr =>
      (cr.value.paths, cr.offset)
    }
    .tap {
      case (paths, offset) =>
        ZIO.foreach(paths) {
          path =>
            for {
              fileData: String <-  ZIO(Source.fromFile(path)).bracket((s: BufferedSource) => URIO(s.close()))(s => ZIO(s.getLines().mkString))
              _ <- zio.console.putStrLn(fileData)
              trades: ZioTrades <- ZIO.fromEither(
                fileData
                  .fromJson[ZioTrades]
                  .left
                  .map(errorMessage => new RuntimeException(errorMessage)))
              filterTradeData: String <- ZIO.effect(ZioTrades(trades.trades.filter(trade => trade.price > 15.0)).toJson)
              uuid: String <- zio.random.nextUUID.mapEffect(data => data.toString)
              filePath: String <- ZIO.succeed(s"src/main/resources/zio/filter-files/$uuid")
              _  <- ZIO.effect(new PrintWriter(new File(filePath))).bracket((s: PrintWriter) => URIO(s.close()))(s => ZIO(s.write(filterTradeData)))
produceMsg: ZIO[Producer[Any, String, String], Throwable, RecordMetadata] <- ZIO.effectTotal{
  val filterrecord: ProducerRecord[String, String] = new ProducerRecord("filterFileTopic", "key-1", "Hwllo filter")
  val filterproducerEffect
  : ZIO[Producer[Any, String, String], Throwable, RecordMetadata] =
    Producer.produce(filterrecord)
  filterproducerEffect

}
              _ <- produceMsg.run


            } yield ()
        }
    }
    .map(_._2) //stream of offsets
    .aggregateAsync(Consumer.offsetBatches)
  def managedSource(file: String): ZManaged[Any, Throwable, BufferedSource] =
    Managed.make(ZIO(Source.fromFile(file)))(s => URIO(s.close))

  def readFileZioManaged(file: String): ZIO[Any, Throwable, Unit] =
    managedSource(file).use(s => ZIO(s.getLines().foreach {
      data => println(data)
    }))
  val streamEffect = streams.run(ZSink.foreach((offset => offset.commit)))

  override def run(args: List[String]) =
    streamEffect.provideSomeLayer(consumer ++ zio.console.Console.live).exitCode


}

object KafkaProducer extends zio.App {

  val filterFilemanagedProducer: RManaged[Blocking, Producer.Service[Any, String, String]] = Producer.make(ProducerSettings(List("localhost:29093")),
    Serde.string,
    Serde.string)
  val filterFileProducer: ZLayer[Blocking, Throwable, Producer[Any, String, String]] =
    ZLayer.fromManaged(filterFilemanagedProducer)

  val filterrecord: ProducerRecord[String, String] = new ProducerRecord("filterFileTopic", "key-1", "Hwllo filter")
  val filterproducerEffect
  : ZIO[Producer[Any, String, String], Throwable, RecordMetadata] =
    Producer.produce(filterrecord)
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    filterproducerEffect.provideSomeLayer(filterFileProducer).exitCode

}