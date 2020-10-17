package com.test

import org.apache.spark.sql.SparkSession

object Historize  {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MyApp")
    .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate()

  def main(args: Array[String]): Unit = {





//     val resultDfToRDD = MusicStats.getTopThreePopularity(MusicDs)
//    resultDfToRDD.show()
//    resultDfToRDD.saveToCassandra("test", "consultants", SomeColumns("id", "lieudemission", "nom", "prenom"))
 }
}
