package com.kainos.enstar.globaldatahub.cdcloader.control

import org.apache.spark.sql.SQLContext

/**
 * Defines expected Behaviour for a control processor.
 */
trait ControlProcessor {

  /**
   * Checks if a table has been previously processed
   *
   * @param sqlContext the SQL Context
   * @param tableName the table to check
   * @return true if previously processed
   */
  def isInitialLoad( sqlContext : SQLContext, tableName : String ) : Boolean

  /**
   * Register a control table for the source system
   */
  def registerControlTable() : Unit

  /**
   * De-register the control table for a source system
   */
  def deregisterControlTable()

  /**
   * get the last attunity change sequence in the control table the sql expected by this method is
   * as follows:
   *
   * SELECT MAX(lastattunitychangeseq) FROM control WHERE attunitytablename =
   *
   * @param tableName the name of the source table being processed
   * @return an attunity change sequence
   */
  def getLastSequenceNumber( tableName : String ) : String

}
