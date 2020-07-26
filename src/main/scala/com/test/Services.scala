package com.test

import com.test.Domains.{Company, Consultant}
import org.apache.spark.sql.{DataFrame, Dataset}

object Services {


  def getOnlyConsultantsInParis(consultantsDS: Dataset[Consultant], companiesDS: Dataset[Company]): DataFrame = {

    val joinedDS = consultantsDS.join(companiesDS, "CompanyName" )
                                .drop("CompanyName")
                                .drop("CompanyId")
    joinedDS.filter("CompanyLocation == 'Paris'")
  }

}

