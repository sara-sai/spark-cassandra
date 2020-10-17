package com.test

import org.apache.spark.sql.SparkSession

object ConnectSparkWithCassandra  {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MyApp")
  //  .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate()

  def main(args: Array[String]): Unit = {



//    val connector = CassandraConnector(spark.sparkContext.getConf)
//    connector.withSessionDo(session =>
//    {
//      session.execute("DROP KEYSPACE IF EXISTS test")
//      session.execute("CREATE KEYSPACE test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
//      session.execute("USE test")
//      session.execute("CREATE TABLE Consultants(id text PRIMARY KEY, nom text , prenom text, lieuDeMission text, )")
//    }
//    )
//
//     val resultDfToRDD = Services.getOnlyConsultantsInParis(employeeDS,companiesDS)
//    resultDfToRDD.show()
   // resultDfToRDD.saveToCassandra("test", "consultants", SomeColumns("id", "lieudemission", "nom", "prenom"))
  }
}
