package edu.knoldus.zio
import java.io.IOException

import zio.{Managed, Task, URIO, ZIO, ZManaged}

import scala.io.{BufferedSource, Source}
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import edu.knoldus.zio.FileReader.parseFile
import zio._
import zio.console.{Console, _}
import zio.stream.{ZSink, ZStream, ZTransducer}

import scala.io.Source

object FileReader extends zio.App {
  val filesPath = List("src/main/resources/newData.json",
    "src/main/resources/newData1.json",
    "src/main/resources/newData2.json")




  val streamFileReader: ZIO[Console, Throwable, Unit] =  {
    val lines: ZStream[Any, Throwable, String] =
      ZStream.fromIteratorManaged(
        ZManaged.fromAutoCloseable(
          Task(scala.io.Source.fromFile("src/main/resources/newData.json"))
        ).map(_.getLines())
      )
    lines.run(ZSink.foreach(data => zio.console.putStrLn(data)))
  }




  def readFileZio(file: String): ZIO[Any, Throwable, String] =
    ZIO(Source.fromFile(file))
      .bracket(
        s => URIO(s.close),
        s => ZIO(s.getLines().mkString)
      )
def managedSource(file: String): ZManaged[Any, Throwable, BufferedSource] =
  Managed.make(ZIO(Source.fromFile(file)))(s => URIO(s.close))

  def parseFile(fileName: String): ZIO[Any, Throwable, Seq[String]] = {
    val path = fileName
    val lines: zio.stream.Stream[Throwable, String] =
      zio.stream.Stream.fromIteratorManaged(
        ZManaged
          .fromAutoCloseable(
            Task(Source.fromFile(path))
          )
          .map(_.getLines())
      )
    lines
      .runCollect
      // apart from formatting, this is actually new (evaluates all Chunks):
      .map(_.toList)
  }

  def readFileZioManaged(file: String): ZIO[Any, Throwable, String] =
  managedSource(file).use(s => ZIO(s.getLines().mkString))
  override def run(args: List[String]) =  streamFileReader.exitCode
}
