package com.test

import org.apache.spark.sql.Encoders

object Domains {

case class Company (CompanyId: String, CompanyName: String, CompanyLocation: String)
case class Consultant (id: String, nom: String, prenom: String, CompanyName: String)

implicit val encodCompany = Encoders.product[Company]
implicit val encodConsulatnt = Encoders.product[Consultant]

}
