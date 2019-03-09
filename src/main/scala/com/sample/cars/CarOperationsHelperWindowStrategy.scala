package com.sample.cars

import com.sample.utils.OperationsHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{levenshtein, _}

class CarOperationsHelperWindowStrategy(_ds: DataFrame,
                                        _keyHashColumns: Seq[Column],
                                        _categoryValuesHashColumns: Seq[Column],
                                        _commentsColumns: (String, String),
                                        levenshteinThreshold: Int
                                       ) extends OperationsHelper {

  override def ds: DataFrame = _ds

  def KeyHashColumns = _keyHashColumns

  def CategoryValuesHashColumns = _categoryValuesHashColumns

  def CommentsColumns = _commentsColumns

  val windowCarsKeyHash = Window.partitionBy(col(CarOperationsHelperWindowStrategy.ColumnKeyHashColumn))
    .orderBy(asc(CarOperationsHelperWindowStrategy.ColumnCategoryFieldsHash), desc(CarOperationsHelperWindowStrategy.ColumnDate))

  val windowCarsCategoryRank = Window.partitionBy(col(CarOperationsHelperWindowStrategy.ColumnKeyHashColumn),
    col(CarOperationsHelperWindowStrategy.ColumnCategoryFieldsHash))
    .orderBy(asc(CarOperationsHelperWindowStrategy.ColumnRank), desc(CarOperationsHelperWindowStrategy.ColumnDate))


  override def preparedDataSet()(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (carsDataDF, colName) =>
      carsDataDF.withColumn(
        colName,
        trim(lower(col(colName)))
      )
    }.na.fill(CarOperationsHelperWindowStrategy.DefaultColumnsValue)
      .withColumn(CarOperationsHelperWindowStrategy.ColumnKeyHashColumn, md5(concat(
        KeyHashColumns: _*
      )))
      .withColumn(CarOperationsHelperWindowStrategy.ColumnCategoryFieldsHash, md5(concat(
        CategoryValuesHashColumns: _*
      )))
      .withColumn(
        CarOperationsHelperWindowStrategy.ConcatComments, concat(col(CommentsColumns._1), col(CommentsColumns._2))
      )
  }

  override def deduplicateDataSet()(df: DataFrame): DataFrame = {
    df
      .withColumn(CarOperationsHelperWindowStrategy.ColumnRank, dense_rank().over(windowCarsKeyHash))
      .withColumn(CarOperationsHelperWindowStrategy.ColumnHashWithDiff,
        concat(col(CarOperationsHelperWindowStrategy.ColumnCategoryFieldsHash),
          when(levenshtein(
            first(CarOperationsHelperWindowStrategy.ConcatComments).over(windowCarsCategoryRank),
            col(CarOperationsHelperWindowStrategy.ConcatComments)) >= levenshteinThreshold, lit("1")).otherwise(lit(""))))
      .withColumn(CarOperationsHelperWindowStrategy.ColumnRowNum, row_number().over(windowCarsCategoryRank))
  }


  override def resultsDataFrame()(df: DataFrame): DataFrame = {
    df.filter(col(CarOperationsHelperWindowStrategy.ColumnRowNum) === 1)
  }

}

object CarOperationsHelperWindowStrategy {

  val DefaultColumnsValue = "default"
  val ColumnKeyHashColumn = "keyHash"
  val ColumnCategoryFieldsHash = "categoryHash"
  val ColumnRank = "rank"
  val ColumnRowNum = "rn"
  val ColumnHashWithDiff = "hashDiff"
  val ConcatComments = "concatComments"
  val ColumnDate = "date"

}
