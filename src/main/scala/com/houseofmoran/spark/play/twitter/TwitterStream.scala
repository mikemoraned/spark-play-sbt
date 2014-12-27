package com.houseofmoran.spark.play.twitter

import java.io.FileReader
import java.util.Properties
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.streaming._
import StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql.SQLContext
import com.github.nscala_time.time.Imports._

object TwitterStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TwitterStream").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("checkpoint")
    val sqlSc = new SQLContext(sc)
    import sqlSc.createSchemaRDD

    val oauthProperties = new Properties()
    oauthProperties.load(new FileReader(args(0)))
    for (key <- oauthProperties.stringPropertyNames()
         if key.startsWith("twitter4j.oauth.")) {
         System.setProperty(key, oauthProperties.getProperty(key))
    }

    val twitterStream = TwitterUtils.createStream(ssc, None)

    val windowLength = Seconds(60)

    val wordStream = twitterStream.flatMap(status => status.getText().split(" "))
    val wordCountStream = wordStream.countByValueAndWindow(windowLength, windowLength)

    wordCountStream.foreachRDD( wordCountRDD => {
      val now = DateTime.now.toDateTimeISO

      wordCountRDD.map( wordCount => (now.toInstant().millis, windowLength.milliseconds, wordCount._1, wordCount._2))
                  .saveAsParquetFile(s"wordcounts/start${ISODateTimeFormat.dateTime().print(now)}_window${windowLength.milliseconds}ms.parquet")
    });

    ssc.start()
    ssc.awaitTermination()
  }
}
