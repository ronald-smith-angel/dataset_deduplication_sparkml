package com.sample.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class ProcessorProductsJob {

}

object ProcessorProductsJob {

  def main(args: Array[String]): Unit = {

    //TODO: Add Exception handling for a production environment.

    val pathProductsFile = args(0)
    val pathHdfsOutput = args(1)

    val master = "local[*]"
    val appName = "products-processor-spark"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)


    val ss = SparkSession.builder
      .config(conf = conf)
      .appName(appName)
      .getOrCreate()


    //TODO: Productionize this job. For now go to the test cases.
  }

}
