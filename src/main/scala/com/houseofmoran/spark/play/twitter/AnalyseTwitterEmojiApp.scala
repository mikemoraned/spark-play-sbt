package com.houseofmoran.spark.play.twitter

import java.util.regex.Pattern

import com.houseofmoran.spark.play.twitter.LoadHelpers._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types._

object AnalyseTwitterEmojiApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyseTwitterEmojiApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)
    import sqlSc._

    val emojiCounts = sqlSc.parquetFiles("emojiusage")

    for(row <- emojiCounts.takeSample(false, 100)) {
      println(row.mkString(","))
    }
  }
}
