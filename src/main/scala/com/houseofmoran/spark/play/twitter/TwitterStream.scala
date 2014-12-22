package com.houseofmoran.spark.play.twitter

import java.io.FileReader
import java.util.Properties
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.streaming._
import StreamingContext._
import org.apache.spark.streaming.twitter._

object TwitterStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val oauthProperties = new Properties()
    oauthProperties.load(new FileReader(args(0)))
    for (key <- oauthProperties.stringPropertyNames()
         if key.startsWith("twitter4j.oauth.")) {
         System.setProperty(key, oauthProperties.getProperty(key))
    }

    val twitterStream = TwitterUtils.createStream(ssc, None)

    val wordStream = twitterStream.flatMap(status => status.getText().split(" ").map((_, 1)))
    val wordCountStream = wordStream.reduceByKeyAndWindow((a: Int, b:Int) => a + b, Seconds(60), Seconds(60))

    wordCountStream.foreachRDD( wordCountRDD => {
        for(wordGroup <- wordCountRDD.groupBy{ case (word, count) => count }.sortBy(_._1)) {
          val count = wordGroup._1
          val words = wordGroup._2.map(_._1).mkString(", ")
          System.out.println(s"$count: $words")
        }
    });

    ssc.start()
    ssc.awaitTermination()
  }
}
