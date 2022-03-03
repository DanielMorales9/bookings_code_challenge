package de.holidaycheck.jobs

import org.apache.spark.sql.SparkSession

abstract class Job[T] {

  implicit val spark: SparkSession

  def init_spark_session(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  def extract(): T

  def transform(df: T): T

  def load(df: T): Unit

  def run(): Unit = {
    val df = extract()
    val transformedDF = transform(df)
    load(transformedDF)
    spark.close()
  }

}
