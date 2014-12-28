package com.houseofmoran.spark.play.twitter

import com.houseofmoran.spark.play.twitter.LoadHelpers._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.{SchemaRDD, Row, SQLContext}

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

    for(row <- mapped.takeSample(false, 100)) {
      println(row)
    }
  }
}
