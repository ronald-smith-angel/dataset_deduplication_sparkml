package com.sample.processor.products

import com.sample.general.SparkSpec
import com.sample.products.OperationsHelperLSH
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}


class ProcessorProductsLshTest extends FlatSpec with SparkSpec with GivenWhenThen with Matchers with BeforeAndAfterEach {

  var productsDataSet: DataFrame = _
  var principalComponentsColumn: Seq[Column] = _
  var nearNeighboursNumber: Int = _
  var hashesNumber: Int = _
  var dsHelper: OperationsHelperLSH = _


  override def beforeEach(): Unit = {
    val spark = ss

    productsDataSet = spark.read.json(getPathString("/products.json.gz"))


    principalComponentsColumn = Seq(col("titleChunk"),
      col("contentChunk"),
      col("color"),
      col("productType"))

    nearNeighboursNumber = 4
    hashesNumber = 3

    dsHelper = new OperationsHelperLSH(productsDataSet,
      principalComponentsColumn, nearNeighboursNumber,
      hashesNumber
    )

  }

  val getPathString = (path: String) => getClass.getResource(path).getPath

  "Starting with a simple join to understand data" should "Recognize Duplicated Data " in {

    val preprocessedDF = dsHelper.ds.transform(dsHelper.preparedDataSet())

    val wordsFilteredTuple = dsHelper.getWordsFilteredDF(preprocessedDF)
    val vectorizedProductsDF = wordsFilteredTuple._1
    val vectorModeler = wordsFilteredTuple._2

    val deduplicateLSHTuple = dsHelper.deduplicateDataSet(vectorizedProductsDF)
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
