package edu.knoldus
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.ExecutionContextExecutor

object CombinedFileConsumer extends App {

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
    * Consume the combinedFileNotification topic
    */
  val kafkaSource: Source[ConsumerMessage.CommittableMessage[String, String],
                          Consumer.Control] = Consumer
    .committableSource(kafkaConsumerSettings,
                       Subscriptions.topics("combinedFileNotification"))
    .map { message =>
      println(message)
      message
    }
  kafkaSource
    .runWith(Sink.foreach(println))

}
