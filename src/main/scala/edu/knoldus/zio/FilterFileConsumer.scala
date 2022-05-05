package edu.knoldus.zio

import java.io.{File, PrintWriter}

import org.apache.kafka.clients.producer.ProducerRecord
import zio.{ExitCode, Has, Managed, RManaged, URIO, ZIO, ZLayer, ZManaged}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.kafka.consumer.{Consumer, ConsumerSettings, OffsetBatch, Subscription}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.random.Random
import zio.stream.{ZSink, ZStream}
import edu.knoldus.zio.ZioReaderConsumer._
import zio.console.Console

import scala.io.{BufferedSource, Source}

object FilterFileConsumer extends zio.App {

  val filterFileManagedConsumer: RManaged[Clock with Blocking, Consumer.Service] = Consumer.make(
    ConsumerSettings(List("localhost:29093"))
      .withGroupId("zio-group"))
  val filterFileConsumer = ZLayer.fromManaged(filterFileManagedConsumer)

  val filterFileStreams = Consumer
    .subscribeAnd(Subscription.topics("filterFileTopic"))
    .plainStream(Serde.string, Serde.string)
    .map { cr =>
      (cr.record.value, cr.offset)
    }
    .tap {
      case (path, offset) =>
       zio.console.putStrLn(path)
    }
    .map(_._2) //stream of offsets
    .aggregateAsync(Consumer.offsetBatches)

  val filterFileStreamEffect = filterFileStreams.run(ZSink.foreach((offset => offset.commit)))

  override def run(args: List[String]) =
    filterFileStreamEffect.provideSomeLayer(consumer ++ zio.console.Console.live).exitCode




}
