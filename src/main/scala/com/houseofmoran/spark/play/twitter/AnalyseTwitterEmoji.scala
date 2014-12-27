package com.houseofmoran.spark.play.twitter

import com.houseofmoran.spark.play.twitter.LoadHelpers._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types._

object AnalyseTwitterEmoji {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyseTwitter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)

    val wordCounts = sqlSc.parquetFiles("wordcounts")

    val schema = StructType(
      StructField("start", TimestampType, false) ::
      StructField("length", IntegerType, false) ::
      StructField("word", StringType, false) ::
      StructField("count", IntegerType, false) :: Nil)

    sqlSc.applySchema(wordCounts, schema).registerTempTable("wordcounts")

    val rows = sqlSc.sql("SELECT * FROM wordcounts LIMIT 10")
    for(row <- rows) {
      println(row.mkString(","))
    }
  }
}
