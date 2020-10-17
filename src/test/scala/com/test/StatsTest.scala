package com.test

import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class StatsTest extends FlatSpec {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MyApp")
    .getOrCreate()



}
