package com.houseofmoran.spark.play.twitter

import java.io.{File, FileReader}
import java.util.Properties

import com.github.nscala_time.time.Imports._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._

object AnalyseTwitter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnalyseTwitter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlSc = new SQLContext(sc)
    import sqlSc.createSchemaRDD

    val wordCountRDDs =
      for(file <- new File("wordcounts").listFiles()
        if file.getName().endsWith(".parquet"))
      yield sqlSc.parquetFile(s"wordcounts/${file.getName()}")

    val wordCounts = sc.union(wordCountRDDs)

    for(row <- wordCounts.takeSample(false, 10)) {
      println(row.mkString(","))
    }
  }
}
