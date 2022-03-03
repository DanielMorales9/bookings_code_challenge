package de.holidaycheck.io

import org.apache.spark.sql.Dataset

object Saver {

  def parquet(
      df: Dataset[_],
      path: String,
      saveMode: String = "error",
      partitionCols: List[String] = List()
  ): Unit = {
    df.write
      .mode(saveMode)
      .partitionBy(partitionCols: _*)
      .parquet(path)
  }

  def csv(
      df: Dataset[_],
      path: String,
      saveMode: String = "error",
      header: Boolean = true,
      partitionCols: List[String] = List()
  ): Unit = {
    df.write
      .mode(saveMode)
      .option("header", header.toString)
      .partitionBy(partitionCols: _*)
      .csv(path)
  }

}
