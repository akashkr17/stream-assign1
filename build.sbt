name := "stream-assign1"

version := "0.1"

scalaVersion := "2.13.8"
val circeVersion = "0.14.1"
val AkkaVersion = "2.6.18"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "org.slf4j" % "slf4j-simple" % "1.7.32",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.0",
  "io.spray" %%  "spray-json" % "1.3.6",
  "com.typesafe.play" %% "play-json" % "2.8.2",
  "com.typesafe.akka" %% "akka-stream-kafka" % "3.0.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-json-streaming" % "3.0.4",
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
)