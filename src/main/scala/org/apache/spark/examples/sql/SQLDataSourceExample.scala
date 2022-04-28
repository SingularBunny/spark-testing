package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object SQLDataSourceExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runJdbcDatasetExample(spark, args)

    spark.stop()
  }

  def runJdbcDatasetExample(spark: SparkSession, args: Array[String]): Unit = {
    val config = parseArguments(args)
    spark
      .read
      .parquet(config("input_path"))
      .write
      .format("jdbc")
      .option("url", config("url"))
      .option("dbtable", config("dbtable"))
      .option("user", config("user"))
      .option("password", config("password"))
      .save()
  }

  def parseArguments(args: Array[String]): Map[String, String] = {
    args.map(_.split("=")).map(pair => (pair(0) -> pair(1))).toMap
  }
}
