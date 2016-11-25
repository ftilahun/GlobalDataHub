package enstar.globaldatahub.cdccontrol.processor

import enstar.globaldatahub.cdccontrol.udfs.UserFunctions
import enstar.globaldatahub.common.io._
import enstar.globaldatahub.common.processor.ControlProcessor
import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * Control processor class
 */
class CDCContolProcessor extends ControlProcessor with Logging {

  /**
   * Register a control table for the source system
   *
   * @param sqlContext     the SQL Context
   * @param reader         CDCDataFrameReader, for reading from a filesystem
   * @param properties     properties file
   * @param tableOperation table operations object, for registering tables
   */
  override def registerControlTable( sqlContext : SQLContext,
                                     reader : DataFrameReader,
                                     properties : GDHProperties,
                                     tableOperation : TableOperations ) : Unit = {
    val path =
      properties.getStringProperty( "spark.cdccontrol.paths.data.control.input" )
    val name =
      properties.getStringProperty( "spark.cdccontrol.tables.control.name" )
    logInfo( "Registering control table from " + path + " as " + name )
    val controlTableDF = reader.read( sqlContext, path, None )
    tableOperation.registerTempTable(
      controlTableDF,
      name
    )
  }

  /**
   * De-register the control table for a source system
   *
   * @param sqlContext     the SQL Context
   * @param properties     properties file
   * @param tableOperation table operations object, for registering tables
   */
  override def deregisterControlTable(
    sqlContext : SQLContext,
    properties : GDHProperties,
    tableOperation : TableOperations ) : Unit = {
    val name =
      properties.getStringProperty( "spark.cdccontrol.tables.control.name" )
    logInfo( "Removing control table:  " + name )
    tableOperation.deRegisterTempTable(
      sqlContext,
      name
    )
  }

  def process( sqlContext : SQLContext,
               reader : DataFrameReader,
               writer : DataFrameWriter,
               sqlFileReader : SQLReader,
               tableOperations : TableOperations,
               properties : GDHProperties,
               userFunctions : UserFunctions ) : Long = {

    logInfo( "Registering the control table" )
    registerControlTable( sqlContext, reader, properties, tableOperations )

    logInfo( "Registering UDFs" )
    userFunctions.registerUDFs( sqlContext, properties )

    val dataPath =
      properties.getStringProperty( "spark.cdccontrol.path.data.input" )
    val tempTableName =
      properties.getStringProperty( "spark.cdccontrol.tables.temp.name" )

    logInfo( "Reading the data directory: " + dataPath )
    val tableData = reader.read(
      sqlContext,
      new Path( dataPath ).toString,
      None
    )

    logInfo( "creating temp table over " + dataPath + " as " + tempTableName )
    tableOperations.registerTempTable( tableData, tempTableName )

    val sqlPath = properties.getStringProperty( "spark.cdccontrol.path.sql" )
    logInfo( "Getting sql from " + sqlPath )
    val sqlStatement =
      sqlFileReader.getSQLString( sqlContext.sparkContext, sqlPath )
    logInfo( "got sql statement: " + sqlStatement )

    logInfo( "Running statement" )
    val controlOutput = sqlContext.sql( sqlStatement )

    val outpath =
      properties.getStringProperty( "spark.cdccontrol.paths.data.control.input" )

    logInfo( "Saving control data to: " + outpath )
    val writtenRows = writer.write( sqlContext,
      outpath,
      controlOutput,
      Some( StorageLevel.MEMORY_AND_DISK ) )
    logInfo( "Wrote " + writtenRows + " rows to control table" )

    logInfo( "Removing temp table" )
    tableOperations.deRegisterTempTable( sqlContext, tempTableName )

    logInfo( "Removing the control table" )
    deregisterControlTable(
      sqlContext,
      properties,
      tableOperations
    )
    writtenRows
  }
}
