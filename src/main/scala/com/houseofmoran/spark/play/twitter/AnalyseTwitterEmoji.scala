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
    import sqlSc._

    val schema = StructType(
      StructField("start", TimestampType, false) ::
      StructField("length", IntegerType, false) ::
      StructField("word", StringType, false) ::
      StructField("c", LongType, false) :: Nil)

    val wordCounts = sqlSc.applySchema(sqlSc.parquetFiles("wordcounts"), schema)

    val filtered = wordCounts.where('word)((w : String) => w.matches("the.+"))
    filtered.registerTempTable("wordcounts")

    val rows = sqlSc.sql("""
      SELECT word,SUM(c) as c
      FROM wordcounts
      GROUP BY word
      ORDER BY c DESC
      LIMIT 10
                         """)

    for(row <- rows) {
      println(row.mkString(","))
    }
  }
}
