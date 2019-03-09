package com.trovit.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class ProcessorCarsJob {

}

object ProcessorCarsJob {

  def main(args: Array[String]): Unit = {

    //TODO: Exceptions handling with Try and Options.
    //TODO:
    val pathCarsFile = args(0)
    val pathHdfsOutput = args(1)

    val master = "local[*]"
    val appName = "cars-processor-spark"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)


    val ss = SparkSession.builder
      .config(conf = conf)
      .appName(appName)
      .getOrCreate()


  }

}
