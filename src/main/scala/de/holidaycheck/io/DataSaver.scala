package de.holidaycheck.io

import org.apache.spark.sql.DataFrame

object DataSaver {

  def parquet(
      df: DataFrame,
      path: String,
      saveMode: String = "error",
      partitionCols: List[String] = List()
  ): Unit = {
    df.write.mode(saveMode).partitionBy(partitionCols: _*).parquet(path)
  }

  def csv(
      df: DataFrame,
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
