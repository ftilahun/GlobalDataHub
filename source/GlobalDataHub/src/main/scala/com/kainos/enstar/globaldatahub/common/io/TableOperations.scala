package com.kainos.enstar.globaldatahub.common.io

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Defines expectded behaviour for registering SparkSQL tables
 */
trait TableOperations {

  /**
   * de-register a temp table
   *
   * @param sqlContext the sql context
   * @param tableName the temp table name.
   */
  def deRegisterTempTable( sqlContext : SQLContext, tableName : String ) : Unit

  /**
   * Register a temp table over a dataframe
   *
   * @param dataFrame a dataframe
   * @param tableName the tablename
   */
  def registerTempTable( dataFrame : DataFrame, tableName : String ) : Unit

}
