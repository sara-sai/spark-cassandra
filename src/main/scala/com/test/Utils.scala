package com.test

import com.test.Domains._
import org.apache.spark.sql.{Dataset, SparkSession}
object Utils {

  def readCSV(path:String)(implicit spark: SparkSession): Dataset[Music] = {
     spark.read
       .option("header", true)
       .csv(path)
       .map(elt => Music(elt.getString(0), elt.getString(1),elt.getString(2),  elt.getString(3),
                        elt.getString(4).toDouble,  elt.getString(5).toInt,  elt.getString(6).toInt,
                        elt.getString(7).toInt,  elt.getString(8).toInt,  elt.getString(9).toInt,
                        elt.getString(10).toInt, elt.getString(11).toInt, elt.getString(12).toInt,
                        elt.getString(13).toInt ))
  }




}



