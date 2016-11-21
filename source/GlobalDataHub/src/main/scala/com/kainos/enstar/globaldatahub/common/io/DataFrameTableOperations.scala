package com.kainos.enstar.globaldatahub.common.io

import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Class for handling Operations on temp tables.
 */
class DataFrameTableOperations
    extends TableOperations
    with Logging {

  /**
   * Register a temp table over a dataframe
   *
   * @param dataFrame a dataframe
   * @param tableName the tablename
   */
  def registerTempTable( dataFrame : DataFrame, tableName : String ) : Unit = {
    logInfo( "registering temp table: " + tableName )
    dataFrame.registerTempTable( tableName )

  }

  /**
   * de-register a temp table
   *
   * @param sqlContext the sql context
   * @param tableName the temp table name.
   */
  def deRegisterTempTable( sqlContext : SQLContext, tableName : String ) : Unit = {
    logInfo( "removing temp table: " + tableName )
    sqlContext.dropTempTable( tableName )
  }
}
