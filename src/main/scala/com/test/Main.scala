package com.test

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

object Main {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MyApp")
    .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)
    connector.withSessionDo(session =>
    {

    println("*********** Connected to Cassandra ***********")

      val path = "src/main/resources/music.csv"
      val musicDS = Utils.readCSV(path)
      val mostPopular = MusicStats.getTopThreePopularity(musicDS)

      session.execute("DROP KEYSPACE IF EXISTS rating")
      session.execute("CREATE KEYSPACE rating WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute("USE rating")
      session.execute("CREATE TABLE rating(track text PRIMARY KEY, artist text, popularity text)")
       mostPopular.rdd.saveToCassandra("rating","rating",SomeColumns("track","artist" ,"popularity"))
    }

    )
  }
}
