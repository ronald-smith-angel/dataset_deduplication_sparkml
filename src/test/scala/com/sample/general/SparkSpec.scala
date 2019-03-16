package com.sample.general

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSpec extends BeforeAndAfterAll {

  this: Suite =>

  private val master = "local[*]"
  private val appName = "example-spark"

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private var _ss: SparkSession = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    _ss = SparkSession.builder
      .config(conf = conf)
      .appName(appName)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_ss != null) {
      _ss.stop()
    }
  }

  def ss: SparkSession = _ss

}
