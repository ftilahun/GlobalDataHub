package com.kainos.enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.cdcloader.io._
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.SQLContext

/**
 * Control processor: Class for interaction with the CDC control table
 * for an input source.
 *
 */
class CDCControlProcessor extends ControlProcessor {

  /**
   * Checks if a table has been previously processed
   *
   * @param sqlContext the SQL Context
   * @param tableName the table to check
   * @param properties properties file
   * @return true if previously processed
   */
  def isInitialLoad( sqlContext : SQLContext,
                     tableName : String,
                     properties : GDHProperties ) : Boolean = {
    val sqlString = s" SELECT ${properties.getArrayProperty( "spark.cdcloader.columns.control.names.controlcolumnnames" ).mkString( "," )} " +
      s"FROM ${properties.getStringProperty( "spark.cdcloader.tables.control.name" )} " +
      s" where ${
        properties.getStringProperty(
          "spark.cdcloader.columns.control.name.tablename" )
      } = $tableName "
    val rows = sqlContext.sql( sqlString )
    val i = rows.count()
    i == 0
  }

  /**
   * Register a control table for the source system
   * @param sqlContext the SQL Context
   * @param reader CDCDataFrameReader, for reading from a filesystem
   * @param properties properties file
   * @param tableOperation table operations object, for registering tables
   */
  def registerControlTable( sqlContext : SQLContext,
                            reader : DataFrameReader,
                            properties : GDHProperties,
                            tableOperation : TableOperations ) : Unit = {
    val controlTableDF = reader.read(
      sqlContext,
      properties.getStringProperty( "spark.cdcloader.paths.data.controlpath" ),
      None )
    tableOperation.registerTempTable(
      controlTableDF,
      properties.getStringProperty( "spark.cdcloader.tables.control.name" ) )
  }

  /**
   * De-register the control table for a source system
   * @param sqlContext the SQL Context
   * @param properties properties file
   * @param tableOperation table operations object, for registering tables
   */
  def deregisterControlTable( sqlContext : SQLContext,
                              properties : GDHProperties,
                              tableOperation : TableOperations ) : Unit =
    tableOperation.deRegisterTempTable(
      sqlContext,
      properties.getStringProperty( "spark.cdcloader.tables.control.name" ) )

  /**
   * get the last attunity change sequence in the control table the sql expected by this method is
   * as follows:
   *
   * SELECT MAX(lastattunitychangeseq) FROM control WHERE attunitytablename =
   *
   * @param sqlContext the SQL Context
   * @param sqlFileReader SQL reader for reading SQL files from the filesystem
   * @param properties properties file
   * @param tableName the name of the source table being processed
   * @return an attunity change sequence or "0" if none found
   */
  def getLastSequenceNumber( sqlContext : SQLContext,
                             sqlFileReader : SQLFileReader,
                             properties : GDHProperties,
                             tableName : String ) : String = {
    val controlTableSQL = sqlFileReader.getSQLString(
      sqlContext.sparkContext,
      properties.getStringProperty( "spark.cdcloader.paths.sql.controlpath" ) )
    val lastSeq =
      sqlContext.sql( controlTableSQL + tableName ).collect()( 0 ).getString( 0 )
    if ( lastSeq == null ) {
      "0"
    } else {
      lastSeq
    }
  }

}
