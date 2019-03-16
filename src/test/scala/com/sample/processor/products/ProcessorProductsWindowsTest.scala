package com.sample.processor.products

import com.sample.entities.Product
import com.sample.general.SparkSpec
import com.sample.products.OperationsHelperWindowStrategy
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}

class ProcessorProductsWindowsTest extends FlatSpec with SparkSpec with GivenWhenThen with Matchers with BeforeAndAfterEach {

  var productsDataSet: DataFrame = _
  var hashKeyColumns: Seq[Column] = _
  var categoryKeyColumns: Seq[Column] = _
  var dsHelper: OperationsHelperWindowStrategy = _

  override def beforeEach(): Unit = {
    val spark = ss

    productsDataSet.write.option("compression", "gzip").json("./sample/")

    hashKeyColumns = Seq(
      col("productType"),
      col("city"),
      col("country"),
      col("region"),
      col("year"),
      col("transmission")
    )

    categoryKeyColumns = Seq(
      col("doors"),
      col("fuel"),
      col("make"),
      col("mileage"),
      col("model"),
      col("color"),
      round(col("price")
      ))

    /** Using Distance 6 for this example */
    dsHelper = new OperationsHelperWindowStrategy(productsDataSet,
      hashKeyColumns, categoryKeyColumns,
      ("titleChunk", "contentChunk"),
      6)

  }

  val getPathString = (path: String) => getClass.getResource(path).getPath

  "Starting with a simple join to understand data" should "Recognize Duplicated Data " in {

    val spark = ss
    import spark.implicits._

    productsDataSet.show(false)

    val deduplicateDF = dsHelper.ds.transform(dsHelper.preparedDataSet())
      .transform(dsHelper.deduplicateDataSet())
      .transform(dsHelper.resultsDataFrame())

    deduplicateDF.cache()

    deduplicateDF.as[Product].show(false)

    val initProductsCount = productsDataSet.count()
    val deduplicateCount = deduplicateDF.count()

    val removedMetric = initProductsCount - deduplicateCount

    assert(removedMetric > (initProductsCount / 3))
  }
}
