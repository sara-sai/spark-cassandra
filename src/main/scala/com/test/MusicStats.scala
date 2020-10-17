package com.test

import com.test.Domains._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object MusicStats {

  def getTopThreePopularity(inputDS: Dataset[Music])(implicit spark: SparkSession): DataFrame = {
    inputDS.sort(desc("Popularity")).select("Track",  "Artist", "Popularity").limit(3)
  }


  def numberTimesArtistAppearsInList(musicDS: Dataset[Music])(implicit spark: SparkSession):Dataset[Artist] = {
    musicDS.createOrReplaceTempView("musicTable")
    spark.sql(
      """
        |select Artist , count(Artist) as count
        |from musicTable
        |group by Artist
        |""".stripMargin
    ).as[Artist]
  }

  //  def numberTimesArtistAppearsInList(ds: Dataset[Domains.Music])(implicit spark: SparkSession): Dataset[Artist] = {
  //    ds.repartition(col("Artist")).groupBy(col("Artist")).count().as[Artist]
  //  }

}

