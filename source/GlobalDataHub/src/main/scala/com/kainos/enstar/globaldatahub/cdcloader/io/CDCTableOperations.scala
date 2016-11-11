package com.kainos.enstar.globaldatahub.cdcloader.io

import org.apache.spark.sql.{ DataFrame, SQLContext }

/**
 * Class for handling Operations on temp tables.
 */
class CDCTableOperations extends TableOperations {

  /**
   * Register a temp table over a dataframe
   *
   * @param dataFrame a dataframe
   * @param tableName the tablename
   */
  def registerTempTable( dataFrame : DataFrame, tableName : String ) : Unit = {
    dataFrame.registerTempTable( tableName )

  }

  /**
   * de-register a temp table
   *
   * @param sqlContext the sql context
   * @param tableName the temp table name.
   */
  def deRegisterTempTable( sqlContext : SQLContext, tableName : String ) : Unit = {
    sqlContext.dropTempTable( tableName )
  }
}
