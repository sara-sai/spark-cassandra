package com.test

import org.apache.spark.sql.Encoders

object Domains {

case class Music (id: String, Track: String, Artist: String,
                   Genre: String, BeatsPerMinute: Double,
                   Energy: Int, Danceability:Int,
                   LoudnessdB: Int,
                   Liveness: Int, Valence: Int,
                   Length: Int, Acousticness: Int,
                   Speechiness: Int, Popularity: Int)

case class Artist (Artist: String, Count: Long)

implicit val encodMusic = Encoders.product[Music]
implicit val encodArtist = Encoders.product[Artist]

}


