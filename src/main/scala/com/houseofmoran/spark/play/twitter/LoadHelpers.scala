package com.houseofmoran.spark.play.twitter

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class ParquetLoadHelper(sqlContext: SQLContext) {
  def parquetFiles(dirName: String): RDD[Row] = {
    val allRDDFileNames =
      for(file <- new File(dirName).listFiles()
          if file.getName().endsWith(".parquet"))
      yield s"$dirName/${file.getName()}"

    return sqlContext.parquetFile(allRDDFileNames.mkString(","))
  }
}

object LoadHelpers {
  implicit def parquetLoadHelper(sqlContext: SQLContext): ParquetLoadHelper = {
    new ParquetLoadHelper(sqlContext)
  }
}
