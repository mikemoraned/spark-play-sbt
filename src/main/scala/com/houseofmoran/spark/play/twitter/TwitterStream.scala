package com.houseofmoran.spark.play.twitter

import java.io.FileReader
import java.util.Properties
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object TwitterStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))

    val oauthProperties = new Properties()
    oauthProperties.load(new FileReader(args(0)))
    for (key <- oauthProperties.stringPropertyNames()
         if key.startsWith("twitter4j.oauth.")) {
         System.setProperty(key, oauthProperties.getProperty(key))
    }

    val twitterStream = TwitterUtils.createStream(ssc, None)

    val bieberStream = twitterStream.filter(status => status.getText().contains("bieber"))

    bieberStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
