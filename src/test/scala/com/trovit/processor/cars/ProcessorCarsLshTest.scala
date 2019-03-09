package com.trovit.processor.cars

import com.trovit.cars.CarOperationsHelperLSH
import com.trovit.general.SparkSpec
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}


class ProcessorCarsLshTest extends FlatSpec with SparkSpec with GivenWhenThen with Matchers with BeforeAndAfterEach {

  var carDataSet: DataFrame = _
  var principalComponentsColumn: Seq[Column] = _
  var nearNeighboursNumber: Int = _
  var hashesNumber: Int = _
  var dsHelper: CarOperationsHelperLSH = _


  override def beforeEach(): Unit = {
    val spark = ss

    carDataSet = spark.read.json(getPathString("/cars.json.gz"))


    principalComponentsColumn = Seq(col("titleChunk"),
      col("contentChunk"),
      col("color"),
      col("carType"))

    nearNeighboursNumber = 4
    hashesNumber = 3

    dsHelper = new CarOperationsHelperLSH(carDataSet,
      principalComponentsColumn, nearNeighboursNumber,
      hashesNumber
    )

  }

  val getPathString = (path: String) => getClass.getResource(path).getPath

  "Starting with a simple join to understand data" should "Recognize Duplicated Data " in {

    val preprocessedDF = dsHelper.ds.transform(dsHelper.preparedDataSet())

    val wordsFilteredTuple = dsHelper.getWordsFilteredDF(preprocessedDF)
    val vectorizedCarsDF = wordsFilteredTuple._1
    val vectorModeler = wordsFilteredTuple._2

    val deduplicateLSHTuple = dsHelper.deduplicateDataSet(vectorizedCarsDF)
    val lshModeledDF = deduplicateLSHTuple._1
    val lshModeler = deduplicateLSHTuple._2

    val filteringResultsCategory = dsHelper.filterResults(lshModeledDF,
      vectorModeler,
      lshModeler,
      ("negro", "tdi")
    )
    filteringResultsCategory.show(false)

    assert(filteringResultsCategory.count() > 2)
  }
}
