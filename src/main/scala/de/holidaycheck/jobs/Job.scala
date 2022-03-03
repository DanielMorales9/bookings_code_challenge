package de.holidaycheck.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Job {

  implicit val spark: SparkSession

  def init_spark_session(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  def extract(): DataFrame

  def transform(df: DataFrame): DataFrame

  def load(df: DataFrame): DataFrame

  def run(): DataFrame = {
    val df = extract()
    val transformedDF = transform(df)
    val finalDF = load(transformedDF)
    spark.close()
    finalDF
  }

}
