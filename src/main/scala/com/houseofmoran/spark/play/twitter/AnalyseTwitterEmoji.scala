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

//    val wordCountsBare = sqlSc.parquetFiles("wordcounts", "start2014-12-27T16.+\\.parquet")
    val wordCountsBare = sqlSc.parquetFiles("wordcounts", "start2014-12-27T1[1-6].+\\.parquet")
    val wordCounts = sqlSc.applySchema(wordCountsBare, schema)

//    def containsEmoji(w : String) : Boolean = {
//      w.matches(""".+[\x{20a0}-\x{32ff}].+""") ||
//        w.matches(""".+[\x{1f000}-\x{1ffff}].+""") ||
//          w.matches(""".+[\x{fe4e5}-\x{fe4ee}].+""")
//    }

    def containsEmoji(w : String) : Boolean = {
//            w.matches(""".+[\x{20a0}-\x{32ff}].+""") //||
              w.matches(""".+[\x{1f000}-\x{1ffff}].+""") //||
//                w.matches(""".+[\x{fe4e5}-\x{fe4ee}].+""")
          }

    val filtered = wordCounts.where('word)(containsEmoji)
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
