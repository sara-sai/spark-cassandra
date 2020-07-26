package com.test

import com.test.Domains.{Company, Consultant}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FlatSpec

class ServicesTest extends FlatSpec {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MyApp")
    .getOrCreate()

  "la méthode qui retourne le dataframe des consultants exercants à Paris" should "ok" in {

    //Given
    val consultant1 = Consultant("01","ROBERT","Jade","edf")
    val consultant2 = Consultant("06","MARTIN","Philippe","bnp")
    val consultant3 = Consultant("03","DUBOIS","Adam","total")
    val consultantsDS = spark.createDataset(Seq(consultant1,consultant2,consultant3))

    val company1 = Company("01","edf","Paris")
    val company2 = Company("03","bnp","Paris")
    val company3 = Company("05","total","Pau")
    val companiesDS = spark.createDataset(Seq(company1,company2,company3))

    val expectedDF: DataFrame = spark.createDataFrame(Seq(("01","ROBERT","Jade","Paris"),("06","MARTIN","Philippe","Paris")))
    expectedDF.show()
    //When
     val resultDF: DataFrame= Services.getOnlyConsultantsInParis(consultantsDS,companiesDS)
    //Then
   assert(expectedDF.collect().sameElements(resultDF.collect()))
  }
}
