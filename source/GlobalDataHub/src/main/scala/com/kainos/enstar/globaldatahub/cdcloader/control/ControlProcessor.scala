package com.kainos.enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.cdcloader.io.{ CDCLoaderIO, CDCSQLReaderIO }
import com.kainos.enstar.globaldatahub.cdcloader.properties.CDCProperties
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/** Control processor: Class for interaction with the CDC control table
  * for an input source.
  *
  * @param sqlContext the SQL Context
  * @param loaderIO CDCLoader, for reading from and writing to a filesystem
  * @param sqlReaderIO SQL reader for reading SQL files from the filesystem
  * @param properties properties file
  */
class ControlProcessor(
    sqlContext : SQLContext,
    loaderIO : CDCLoaderIO,
    sqlReaderIO : CDCSQLReaderIO,
    properties : CDCProperties ) {

  /**
    * Register a control table for the source system
    */
  def registerControlTable : Unit = {
    val controlTableDF = loaderIO.read( sqlContext, properties.getStringProperty("controlTablePath"), None )
    loaderIO.registerTempTable( controlTableDF, properties.getStringProperty("controlTableName") )
  }

  /**
    * De-register the control table for a source system
    */
  def deregisterControlTable : Unit = loaderIO.deRegisterTempTable( sqlContext, properties.getStringProperty("controlTableName") )

  /** get the last attunity change sequence in the control table the sql expected by this method is
    * as follows:
    *
    * SELECT MAX(lastattunitychangeseq) FROM control WHERE attunitytablename =
    *
    * @param tableName the name of the source table being processed
    * @return an attunity change sequence
    */
  def getLastSequenceNumber( tableName : String ) : String = {
    val controlTableSQL = sqlReaderIO.getSQLString( sqlContext.sparkContext, properties.getStringProperty("controlTableSQLPath") )
    sqlContext.sql(controlTableSQL + tableName).collect()(0).getString(0)
  }

  /** Generate an attunity change sequence for a table.
    * this sequence should be used when processing the initial load table.
    * @return
    */
  def generateFirstSequenceNumber : String = {
    DateTimeFormat.forPattern( properties.getStringProperty("changeSequenceTimestampFormat") ).print( new DateTime() ) +
    "0000000000000000000"
  }

}
