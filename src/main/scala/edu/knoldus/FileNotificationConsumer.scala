package edu.knoldus

import java.nio.file.Paths
import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
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

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.BufferedSource
import scala.util.Using
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
  val producerConfig: Config = config.getConfig("akka.kafka.producer")
  val newProducerSettings: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)

  /**
    * Consume the fileNotification topic and create a window of getting messages produced in 10 second in a sequence
    */
  val kafkaSource: Source[Seq[(String, Long)], Consumer.Control] = Consumer
    .committableSource(kafkaConsumerSettings,
                       Subscriptions.topics("fileNotification"))
    .groupedWithin(1000, 10.seconds) //get the produced message in 10 secound and limit 10000
    .map { messageSeq =>
      messageSeq.map { message =>
        (message.record.value, message.record.timestamp)
      }
    }

  /**
    * Flow get the contents of the files using the sequence of paths
    */
  val getFilesContentFlow
    : Flow[Seq[(String, Long)], Seq[(Trades, Long)], NotUsed] =
    Flow[Seq[(String, Long)]].map { messageData =>
      messageData.map { messageTuple =>
        Using
          .Manager { use =>
            val fileContent: BufferedSource =
              use(scala.io.Source.fromFile(messageTuple._1))
            fileContent.mkString.parseJson.convertTo[Trades] match {
              case fileData: Trades =>
                (fileData, messageTuple._2)
              case _ =>
                (Trades(Nil), 0L)
            }
          }
          .getOrElse((Trades(Nil), 0L))
      }
    }

  /**
    * Flow filter the combined data of all the files
    */
  val filterCombineDataFlow
    : Flow[Seq[(Trades, Long)], List[(String, List[Prices])], NotUsed] =
    Flow[Seq[(Trades, Long)]].map { tradesTimeStampTupleSeq =>
      tradesTimeStampTupleSeq.flatMap { tradesTimeStampTuple =>
        tradesTimeStampTuple._1.trades
          .groupBy(_.symbol)
          .map { mappedTradesData =>
            val price: Seq[Prices] = mappedTradesData._2.map(trade =>
              Prices(trade.price, tradesTimeStampTuple._2))
            (mappedTradesData._1, price.toList)
          }
      }.toList

    }

  val createFilterDataFileFlow
    : Flow[List[(String, List[Prices])], Future[IOResult], NotUsed] = {
    Flow[List[(String, List[Prices])]].map { sameTradeTupleList =>
      val cd: CombineTrades = CombineTrades(
        sameTradeTupleList.map(sameTradeTuple =>
          SameTrade(sameTradeTuple._1, sameTradeTuple._2))
      )
      val uuid = UUID.randomUUID().toString
      val filePath = Paths.get(s"src/main/resources/$uuid")
      println(filePath)
      Source
        .single(ByteString(cd.toJson.toString))
        .runWith(FileIO.toPath(filePath))
        .map { response =>
          if (response.status.isSuccess) {
            println("success")
            produceCombinedFileMessage(filePath.toString)
          }
          response
        }
    }
  }

  /**
    * Function produce message -combinedFileLocation Message
    */
  def produceCombinedFileMessage(filePath: String) = {
    val sourceData = List(filePath)
    Source(sourceData)
      .map(loc =>
        new ProducerRecord[String, String]("combinedFileNotification", loc))
      .runWith(Producer.plainSink(newProducerSettings))
  }

  kafkaSource
    .via(getFilesContentFlow)
    .via(filterCombineDataFlow)
    .via(createFilterDataFileFlow)
    .runWith(Sink.foreach(println))

}
