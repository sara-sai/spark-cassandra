package com.test

import com.test.Domains.{Artist, Music}
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class MusicStatsTest extends FlatSpec {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MyApp")
    .getOrCreate()

  " The numberTimesArtistAppearsInList function" should ("return the number of times the artist appears in the list") in {
   //Given
    val row1 = Music("32","7 rings","Ariana Grande","dance pop",140,32,78,-11,9,33,179,59,33,89)
    val row2 = Music("3","boyfriend (with Social House)","Ariana Grande","dance pop",190,80,40,-4,16,70,186,12,46,85)
    val row3 = Music("20","Truth Hurts","Lizzo","escape room",158,62,72,-3,12,41,173,11,11,91)
    val ds = spark.createDataset(Seq(row1, row2, row3))

    val expectedlist1= Artist("Ariana Grande",2)
    val expectedlist2=Artist ("Lizzo",1)
    val expected = spark.createDataset(Seq(expectedlist1,expectedlist2))

    //When
    val result = MusicStats.numberTimesArtistAppearsInList(ds)

    //Then
    assert(result.collect().sameElements(expected.collect()))
  }

  "The topThreePopularity function" should ("return a df that contains the top 3 of popular songs") in {
    //Given
    val row1 = Music("7","Ransom","Lil Tecca","trap music",180,64,75,-6,7,23,131,2,29,92)
    val row2 = Music("8","How Do You Sleep?","Sam Smith","pop",111,68,48,-5,8,35,202,15,9,90)
    val row3 = Music("20","Truth Hurts","Lizzo","escape room",158,62,72,-3,12,41,173,11,11,91)
    val row4 = Music( "11","Callaita","Bad Bunny","reggaeton",176,62,61,-5,24,24,251,60,31,93)
    val row5 = Music("9","Old Town Road - Remix","Lil Nas X","country rap",136,62,88,-6,11,64,157,5,10,87)
    val row6 = Music("10","bad guy","Billie Eilish","electropop",135,43,70,-11,10,56,194,33,38,95)
    val inputDS = spark.createDataset(Seq(row1,row2,row3,row4,row5,row6))

    val expectedDF= spark.createDataFrame(Seq(
      ("bad guy","Billie Eilish", 95),("Callaita","Bad Bunny", 93),("Ransom","Lil Tecca",92)))

    //When
    val resultDS = MusicStats.getTopThreePopularity(inputDS)

    //Then
    assert(resultDS.collect().sameElements(expectedDF.collect()))
  }

  }
