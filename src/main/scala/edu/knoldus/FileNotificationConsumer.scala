package edu.knoldus

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink, Source}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import edu.knoldus.TradesFormat._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.io.BufferedSource
import scala.util.{Failure, Success, Try, Using}
import spray.json._

import scala.util.Using.Releasable
object FileNotificationConsumer extends App {
  implicit val system: ActorSystem = ActorSystem("consumer-sys")
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val kafkaConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:29092")
      .withGroupId("group1")
  val config: Config = ConfigFactory.load()
  val consumerConfig: Config = config.getConfig("akka.kafka.consumer")

  /**
    * Consume the fileNotification topic and create a window of getting messages produced in 10 second in a sequence
    */
  val kafkaSource = Consumer
    .committableSource(kafkaConsumerSettings,
                       Subscriptions.topics("fileNotification"))
    .groupedWithin(1000, 10.seconds) //get the produced message in 10 secound and limit 10000
    .map { messageSeq =>
      messageSeq.map { message =>
        message.record.value
      }
    }

  /**
    * Flow get the contents of the files using the sequence of paths
    */
  val getFilesContentFlow = Flow[Seq[String]].map { fileLoc =>
    fileLoc.map { loc =>
      Using
        .Manager { use =>
          val fileContent: BufferedSource = use(scala.io.Source.fromFile(loc))
          fileContent.mkString.parseJson.convertTo[Trades] match {
            case data: Trades =>
              println(data)
              data
            case _ =>
              println("From False")
              Trades(Nil)
          }
        }
        .getOrElse(Trades(Nil))
    }
  }

  /**
    * Testing code for combine file Data using a temporary file
    */
  val combineFilesDataFlow = Flow[Seq[Trades]].mapAsync(parallelism = 4) {
    tradesSeq =>
      Source
        .single(ByteString(tradesSeq.toJson.toString))
        .runWith(FileIO.toPath(Paths.get("src/main/resources/newFile2.json")))
  }

  kafkaSource
    .via(getFilesContentFlow)
    .via(combineFilesDataFlow)
    .runWith(Sink.foreach(println))

}
