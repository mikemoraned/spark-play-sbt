package com.houseofmoran.spark.play.twitter

import com.houseofmoran.spark.play.twitter.LoadHelpers._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql.{Row, SQLContext}

object AnalyseTwitterEmojiApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyseTwitterEmojiApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)

    val emojiCounts = sqlSc.parquetFiles("emojiusage")
    emojiCounts.printSchema()

    val mapped = emojiCounts.map((row) => {
      val wordEmojiPairStruct = row.getAs[Row](2)
      val count = row.getLong(3)

      ((wordEmojiPairStruct.getString(0), Emoji(wordEmojiPairStruct.getAs[Row](1).getString(0))), count)
    })
    val rolledUp = mapped.reduceByKey(_ + _).sortBy({ case (_, count) => count }, false)

    for(entry <- rolledUp.sortBy({ case (_, count) => count }, false).take(10)) {
      println(entry)
    }

    val wordCounts = rolledUp.map{ case ((word, emoji), count) => (word, count)}.reduceByKey(_ + _)

    for(entry <- wordCounts.sortBy({ case (_, count) => count }, false).take(10)) {
      println(entry)
    }
  }
}
