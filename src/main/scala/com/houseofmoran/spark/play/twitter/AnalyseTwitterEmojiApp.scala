package com.houseofmoran.spark.play.twitter

import java.util.regex.Pattern

import com.houseofmoran.spark.play.twitter.LoadHelpers._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types._

object AnalyseTwitterEmojiApp {
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

    val wordCountsBare = sqlSc.parquetFiles("wordcounts", "start2014-12-27T1[1-6].+\\.parquet")
    val wordCounts = sqlSc.applySchema(wordCountsBare, schema)

    val emojiRange = """[\x{1f000}-\x{1ffff}]"""
    def containsEmoji(w : String) : Boolean = {
      w.matches(s".+$emojiRange.+")
    }

    val filtered = wordCounts.where('word)(containsEmoji)
//    filtered.registerTempTable("wordcounts")

    val emojiCounts = filtered.flatMap((row) => {
      if (row(2) == null) {
        Seq((null, 0))
      }
      else {
        val word = row.getString(2)
        val count = row.getLong(3)

        val regex = s"($emojiRange)".r
        val emoji = (for(regex(e) <- regex.findAllIn(word)) yield e).toTraversable
        val emojiCounts = emoji.
          groupBy((s: String) => s).
          map((group) => (group._1, group._2.toList.length * count))

        emojiCounts
      }
    })

    println(emojiCounts.takeSample(false, 100).mkString("\n"))

//    val emojiSql = sqlSc.applySchema(emojiCounts, StructType(
//        StructField("emoji", StringType, true) ::
//        StructField("c", LongType, false) :: Nil))
//    emojiSql.registerTempTable("emojicounts")
//
//    val rows = sqlSc.sql("""
//      SELECT emoji,SUM(c) as c
//      FROM emojicounts
//      GROUP BY emoji
//      ORDER BY c DESC
//      LIMIT 20
//                         """)

//    val rows = sqlSc.sql("""
//      SELECT word,SUM(c) as c
//      FROM wordcounts
//      GROUP BY word
//      ORDER BY c DESC
//      LIMIT 10
//                         """)

//    for(row <- rows) {
//      println(row.mkString(","))
//    }
  }
}
