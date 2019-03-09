package com.trovit.utils

import org.apache.spark.sql.DataFrame

trait OperationsHelper {
  def ds: DataFrame

  def preparedDataSet()(df: DataFrame): DataFrame

  def deduplicateDataSet()(df: DataFrame): DataFrame

  def resultsDataFrame()(df: DataFrame): DataFrame
}
