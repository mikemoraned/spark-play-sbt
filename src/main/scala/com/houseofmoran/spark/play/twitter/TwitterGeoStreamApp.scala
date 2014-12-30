package com.houseofmoran.spark.play.twitter

import com.github.nscala_time.time.Imports._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object TwitterGeoStreamApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterGeoStream").setMaster("local[*]")
    val sc = new SparkContext(conf)
    implicit val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("checkpoint")
    val sqlSc = new SQLContext(sc)
    import sqlSc.createSchemaRDD

    val twitterStream = TwitterStreamSource.streamFromAuthIn(args(0))

    val stream = twitterStream.filter(status => status.getGeoLocation() != null)

    stream.foreachRDD( rdd => {
      rdd.takeSample(true, 10).foreach(println)
    });

    ssc.start()
    ssc.awaitTermination()
  }
}
