package com.houseofmoran.spark.play.twitter

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class ParquetLoadHelper(sqlContext: SQLContext) {
  def parquetFiles(dirName: String): RDD[Row] = {
    val allRDDs =
      for(file <- new File(dirName).listFiles()
          if file.getName().endsWith(".parquet"))
      yield sqlContext.parquetFile(s"$dirName/${file.getName()}")

    return sqlContext.sparkContext.union(allRDDs)
  }
}

object LoadHelpers {
  implicit def parquetLoadHelper(sqlContext: SQLContext): ParquetLoadHelper = {
    new ParquetLoadHelper(sqlContext)
  }
}
