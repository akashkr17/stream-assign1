package edu.knoldus

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object FileReaderProducer extends App {

  implicit val system: ActorSystem = ActorSystem("producer-sys")
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val config: Config = ConfigFactory.load()
  val producerConfig: Config = config.getConfig("akka.kafka.producer")
  val producerSettings: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)

  /***
    * Produce the Topic fileReader with Location of the Trades Files
    */
  val produce: Future[Done] = {
    val filesPath = List("src/main/resources/newData.json",
                         "src/main/resources/newData1.json",
                         "src/main/resources/newData2.json")
    Source(filesPath)
      .map(path => new ProducerRecord[String, String]("fileReader", path))
      .runWith(Producer.plainSink(producerSettings))
  }
  produce onComplete {
    case Success(_)   => println("Done"); system.terminate()
    case Failure(err) => println(err.toString); system.terminate()
  }
}
