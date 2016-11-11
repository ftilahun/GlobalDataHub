package com.kainos.enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.cdcloader.io._
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Control processor: Class for interaction with the CDC control table
 * for an input source.
 *
 * @param sqlContext the SQL Context
 * @param reader CDCDataFrameReader, for reading from a filesystem
 * @param sqlFileReader SQL reader for reading SQL files from the filesystem
 * @param properties properties file
 */
class CDCControlProcessor( sqlContext : SQLContext,
                           reader : DataFrameReader,
                           sqlFileReader : SQLFileReader,
                           properties : GDHProperties,
                           tableOperation : TableOperations ) {

  /**
   * Checks if a table has been previously processed
   *
   * @param sqlContext the SQL Context
   * @param tableName the table to check
   * @return true if previously processed
   */
  def isInitialLoad( sqlContext : SQLContext, tableName : String ) : Boolean = {
    val sqlString = s" SELECT * FROM ${properties.getStringProperty( "controlTableName" )} " +
      s" where ${properties.getStringProperty( "attunitytablenameColumn" )} = $tableName "
    val rows = sqlContext.sql( sqlString )
    rows.foreach( println )
    val i = rows.count()
    i == 0
  }

  /**
   * Register a control table for the source system
   */
  def registerControlTable() : Unit = {
    val controlTableDF = reader
      .read( sqlContext, properties.getStringProperty( "controlTablePath" ), None )
    tableOperation.registerTempTable(
      controlTableDF,
      properties.getStringProperty( "controlTableName" ) )
  }

  /**
   * De-register the control table for a source system
   */
  def deregisterControlTable() : Unit =
    tableOperation.deRegisterTempTable(
      sqlContext,
      properties.getStringProperty( "controlTableName" ) )

  /**
   * get the last attunity change sequence in the control table the sql expected by this method is
   * as follows:
   *
   * SELECT MAX(lastattunitychangeseq) FROM control WHERE attunitytablename =
   *
   * @param tableName the name of the source table being processed
   * @return an attunity change sequence
   */
  def getLastSequenceNumber( tableName : String ) : String = {
    val controlTableSQL = sqlFileReader.getSQLString(
      sqlContext.sparkContext,
      properties.getStringProperty( "controlTableSQLPath" ) )
    sqlContext.sql( controlTableSQL + tableName ).collect()( 0 ).getString( 0 )
  }

  /**
   * Generate an attunity change sequence for a table.
   * this sequence should be used when processing the initial load table.
   * @return
   */
  def generateFirstSequenceNumber : String = {
    DateTimeFormat
      .forPattern(
        properties.getStringProperty( "changeSequenceTimestampFormat" ) )
      .print( new DateTime() ) +
      "0000000000000000000"
  }

}
