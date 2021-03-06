package de.holidaycheck.transformations

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()

  }

}
