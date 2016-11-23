package enstar.globaldatahub.cdcloader.control

import com.kainos.enstar.globaldatahub.cdcloader.io._
import com.kainos.enstar.globaldatahub.common.io.{DataFrameReader, SQLReader, TableOperations}
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import enstar.globaldatahub.common.io.{DataFrameReader, SQLReader, TableOperations}
import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

/**
 * Control processor: Class for interaction with the CDC control table
 * for an input source.
 *
 */
class CDCControlProcessor extends ControlProcessor with Logging {

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
    logInfo( "Running statment: " + sqlString )
    val rows = sqlContext.sql( sqlString )
    val i = rows.count()
    logInfo( "Got " + i + " rows" )
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
    val path = properties.getStringProperty( "spark.cdcloader.path.data.controlpath" )
    val name = properties.getStringProperty( "spark.cdcloader.tables.control.name" )
    logInfo( "Registering control table from " + path + " as " + name )
    val controlTableDF = reader.read(
      sqlContext,
      path,
      None )
    tableOperation.registerTempTable(
      controlTableDF, name
    )
  }

  /**
   * De-register the control table for a source system
   * @param sqlContext the SQL Context
   * @param properties properties file
   * @param tableOperation table operations object, for registering tables
   */
  def deregisterControlTable( sqlContext : SQLContext,
                              properties : GDHProperties,
                              tableOperation : TableOperations ) : Unit = {
    val name = properties.getStringProperty( "spark.cdcloader.tables.control.name" )
    logInfo( "Removing control table:  " + name )
    tableOperation.deRegisterTempTable(
      sqlContext, name
    )
  }

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
                             sqlFileReader : SQLReader,
                             properties : GDHProperties,
                             tableName : String ) : String = {
    val path = properties.getStringProperty( "spark.cdcloader.path.sql.controlpath" )
    logInfo( "Getting sql statement from: " + path )
    val controlTableSQL = sqlFileReader.getSQLString(
      sqlContext.sparkContext, path
    )
    logInfo( "Running sql statement: " + controlTableSQL + tableName )
    val lastSeq =
      sqlContext.sql( controlTableSQL + tableName ).collect()( 0 ).getString( 0 )
    if ( lastSeq == null ) {
      logInfo( "No last sequence found, returning 0" )
      "0"
    } else {
      logInfo( "Last sequence was: " + lastSeq )
      lastSeq
    }
  }
}
