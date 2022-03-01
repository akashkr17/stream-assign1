package edu.knoldus

import java.nio.file.Paths
import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{
  ConsumerMessage,
  ConsumerSettings,
  ProducerSettings,
  Subscriptions
}
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}
import edu.knoldus.TradesFormat._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import spray.json._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.BufferedSource
import scala.util.Using

object FileReaderConsumer extends App {
  implicit val system: ActorSystem = ActorSystem("consumer-sys")
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val kafkaConsumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:29092")
      .withGroupId("group1")

  val config = ConfigFactory.load()
  val consumerConfig = config.getConfig("akka.kafka.consumer")
  val producerConfig: Config = config.getConfig("akka.kafka.producer")
  val newProducerSettings: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)

  /**
    * Consume the fileReader Topic using CommittableSource
    */
  val kafkaSource: Source[ConsumerMessage.CommittableMessage[String, String],
                          Consumer.Control] = Consumer
    .committableSource(kafkaConsumerSettings,
                       Subscriptions.topics("fileReader"))

  /**
    * Flow get the value of the message- File Location
    */
  val filePathDecodeFlow = Flow[CommittableMessage[String, String]].map {
    message =>
      message.record.value
  }

  /**
    * Flow read the given file using location
    */
  val fileReaderFlow: Flow[String, Trades, NotUsed] = Flow[String].map {
    filePath =>
      Using
        .Manager { use =>
          val fileContent: BufferedSource =
            use(scala.io.Source.fromFile(filePath))
          fileContent.mkString.parseJson.convertTo[Trades] match {
            case data: Trades =>
              println(s"Data: $data")
              data
            case _ =>
              println("From False")
              Trades(Nil)
          }
        }
        .getOrElse(Trades(Nil))
  }

  /**
    * Flow filter the data which have price greater than equal to 15
    */
  val filterContentFlow: Flow[Trades, Trades, NotUsed] = Flow[Trades].map {
    trades =>
      println(trades)
      Trades(trades.trades.filter(_.price >= 15))
  }

  /**
    * Flow convert the trades data into ByteString
    */
  val byteStringConverterFlow: Flow[Trades, ByteString, NotUsed] =
    Flow[Trades].map { trades =>
      ByteString(trades.toJson.toString)
    }

  /**
    * Flow create the files for filter data and produce the fileNotification topic with message-newly created files
    */
  val fileFlow: Flow[ByteString, IOResult, NotUsed] = {

    Flow[ByteString].mapAsync(parallelism = 4) { s ⇒
      val uuid = UUID.randomUUID().toString
      val filePath = Paths.get(s"src/main/resources/$uuid")
      Source.single(s).runWith(FileIO.toPath(filePath)).map { response =>
        if (response.status.isSuccess) {
          produceFileLocMessage(filePath.toString)
        }
        response
      }
    }
  }

  /**
    * Function produce message -file Location
    */
  def produceFileLocMessage(filePath: String): Future[Done] = {
    val sourceData = List(filePath)
    Source(sourceData)
      .map(loc => new ProducerRecord[String, String]("fileNotification", loc))
      .runWith(Producer.plainSink(newProducerSettings))
  }

  kafkaSource
    .via(filePathDecodeFlow)
    .via(fileReaderFlow)
    .via(filterContentFlow)
    .via(byteStringConverterFlow)
    .via(fileFlow)
    .runWith(Sink.foreach(println))

}
