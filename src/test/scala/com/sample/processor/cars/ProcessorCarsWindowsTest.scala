package com.sample.processor.cars

import com.sample.cars.CarOperationsHelperWindowStrategy
import com.sample.entities.Car
import com.sample.general.SparkSpec
import org.apache.spark.sql.functions.{col, round}
import org.apache.spark.sql.{Column, DataFrame}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, GivenWhenThen, Matchers}

class ProcessorCarsWindowsTest extends FlatSpec with SparkSpec with GivenWhenThen with Matchers with BeforeAndAfterEach {

  var carDataSet: DataFrame = _
  var hashKeyColumns: Seq[Column] = _
  var categoryKeyColumns: Seq[Column] = _
  var dsHelper: CarOperationsHelperWindowStrategy = _

  override def beforeEach(): Unit = {
    val spark = ss

    carDataSet.write.option("compression", "gzip").json("./sample/")

    print(carDataSet.count())

    hashKeyColumns = Seq(
      col("carType"),
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

    dsHelper = new CarOperationsHelperWindowStrategy(carDataSet,
      hashKeyColumns, categoryKeyColumns,
      ("titleChunk", "contentChunk"),
      6)

  }

  val getPathString = (path: String) => getClass.getResource(path).getPath

  "Starting with a simple join to understand data" should "Recognize Duplicated Data " in {

    val spark = ss
    import spark.implicits._

    carDataSet.show(false)

    val deduplicateDF = dsHelper.ds.transform(dsHelper.preparedDataSet())
      .transform(dsHelper.deduplicateDataSet())
      .transform(dsHelper.resultsDataFrame())

    deduplicateDF.cache()

    deduplicateDF.as[Car].show(false)

    val initCarsCount = carDataSet.count()
    val deduplicateCount = deduplicateDF.count()

    val removedMetric = initCarsCount - deduplicateCount

    assert(removedMetric > (initCarsCount / 3))
  }
}
