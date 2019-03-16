package com.sample.products

import com.sample.utils.OperationsHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{levenshtein, _}
import org.apache.spark.sql.{Column, DataFrame}

class OperationsHelperWindowStrategy(_ds: DataFrame,
                                     _keyHashColumns: Seq[Column],
                                     _categoryValuesHashColumns: Seq[Column],
                                     _commentsColumns: (String, String),
                                     levenshteinThreshold: Int
                                    ) extends OperationsHelper {

  val windowProductKeyHash = Window.partitionBy(col(OperationsHelperWindowStrategy.ColumnKeyHashColumn))
    .orderBy(asc(OperationsHelperWindowStrategy.ColumnCategoryFieldsHash), desc(OperationsHelperWindowStrategy.ColumnDate))

  val windowProductsCategoryRank = Window.partitionBy(col(OperationsHelperWindowStrategy.ColumnKeyHashColumn),
    col(OperationsHelperWindowStrategy.ColumnCategoryFieldsHash))
    .orderBy(asc(OperationsHelperWindowStrategy.ColumnRank), desc(OperationsHelperWindowStrategy.ColumnDate))

  override def ds: DataFrame = _ds

  /**
    * Clean and Prepares all the non-fuzzy columns for the dataset. Fills nulls, trim , normalize and generates hash using MD5
    * for non-fuzzy column.
    *
    * @param df
    * @return
    */
  override def preparedDataSet()(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (productsDataDF, colName) =>
      productsDataDF.withColumn(
        colName,
        trim(lower(col(colName)))
      )
    }.na.fill(OperationsHelperWindowStrategy.DefaultColumnsValue)
      .withColumn(OperationsHelperWindowStrategy.ColumnKeyHashColumn, md5(concat(
        KeyHashColumns: _*
      )))
      .withColumn(OperationsHelperWindowStrategy.ColumnCategoryFieldsHash, md5(concat(
        CategoryValuesHashColumns: _*
      )))
      .withColumn(
        OperationsHelperWindowStrategy.ConcatComments, concat(col(CommentsColumns._1), col(CommentsColumns._2))
      )
  }

  def KeyHashColumns = _keyHashColumns

  def CategoryValuesHashColumns = _categoryValuesHashColumns

  def CommentsColumns = _commentsColumns

  /**
    * Applies windows functions and Levenshtein to group similar categories.
    *
    * @param df
    * @return
    */
  override def deduplicateDataSet()(df: DataFrame): DataFrame = {
    df
      .withColumn(OperationsHelperWindowStrategy.ColumnRank, dense_rank().over(windowProductKeyHash))
      .withColumn(OperationsHelperWindowStrategy.ColumnHashWithDiff,
        concat(col(OperationsHelperWindowStrategy.ColumnCategoryFieldsHash),
          when(levenshtein(
            first(OperationsHelperWindowStrategy.ConcatComments).over(windowProductsCategoryRank),
            col(OperationsHelperWindowStrategy.ConcatComments)) >= levenshteinThreshold, lit("1")).otherwise(lit(""))))
      .withColumn(OperationsHelperWindowStrategy.ColumnRowNum, row_number().over(windowProductsCategoryRank))
  }


  /**
    * Deduplicate Similar entities < levenshteinThreshold.
    *
    * @param df
    * @return
    */
  override def resultsDataFrame()(df: DataFrame): DataFrame = {
    df.filter(col(OperationsHelperWindowStrategy.ColumnRowNum) === 1)
  }

}

object OperationsHelperWindowStrategy {

  val DefaultColumnsValue = "default"
  val ColumnKeyHashColumn = "keyHash"
  val ColumnCategoryFieldsHash = "categoryHash"
  val ColumnRank = "rank"
  val ColumnRowNum = "rn"
  val ColumnHashWithDiff = "hashDiff"
  val ConcatComments = "concatComments"
  val ColumnDate = "date"

}
