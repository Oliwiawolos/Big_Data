package agh.wggios.analizadanych.datareader

import agh.wggios.analizadanych
import org.apache.spark.sql.DataFrame
import agh.wggios.analizadanych.{LoggingUtils, SparkSessionProvider}
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Paths}

class DataReader extends SparkSessionProvider {

  def read_csv(path: String): DataFrame =  {
    logInfo("CZYTAM SOBIE PLIK")
    spark.read.format("csv").option("header", value = true).option("inferSchema", value = true).load(path)

  }
  def transformData(df: DataFrame): DataFrame = {
    logInfo("TRANSFORMUJĘ")
    df.filter("count_views > 1000")
   }

  def saveData(df: DataFrame, outputPath: String): Unit = {
    logInfo(s"Zapisuję dane do: $outputPath")
    df.write.mode("overwrite").parquet(outputPath)
   }

}
