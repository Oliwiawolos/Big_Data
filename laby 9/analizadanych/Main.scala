package agh.wggios.analizadanych

import agh.wggios.analizadanych.datareader.DataReader

object Main extends SparkSessionProvider {
  LoggingUtils.setupLogging()

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    logInfo("Startuję program")
    val reader = new DataReader()

    val df = reader.read_csv(args(0))
    val transformed = reader.transformData(df)
    reader.saveData(transformed, args(1))

    logInfo("Koniec programu")
  }
}

