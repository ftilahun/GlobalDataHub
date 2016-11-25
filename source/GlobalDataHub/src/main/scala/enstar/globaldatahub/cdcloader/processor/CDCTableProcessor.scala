package enstar.globaldatahub.cdcloader.processor

import enstar.globaldatahub.cdcloader.control.ControlProcessor
import enstar.globaldatahub.cdcloader.udfs.UserFunctions
import enstar.globaldatahub.common.io.{
  DataFrameReader,
  DataFrameWriter,
  SQLReader,
  TableOperations
}
import enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * Process a source table
 */
class CDCTableProcessor extends TableProcessor with Logging {

  /**
   * Process an source table
   *
   * @param tableName the name of the table to process
   * @param sqlContext the sql context
   * @param controlProcessor the control processor
   * @param properties the properties object
   * @param reader a dataframe reader
   * @param userFunctiions a udfs object
   * @param tableOperations a table operations object
   * @param sqlReader a sqlreader object
   * @return a dataframe of the source table
   */
  def process( tableName : String,
               sqlContext : SQLContext,
               controlProcessor : ControlProcessor,
               properties : GDHProperties,
               reader : DataFrameReader,
               userFunctiions : UserFunctions,
               tableOperations : TableOperations,
               sqlReader : SQLReader ) : DataFrame = {

    logInfo( "Loading data for table: " + tableName )
    val data = load( tableName,
      sqlContext,
      controlProcessor,
      reader,
      userFunctiions,
      properties )
    val path = properties.getStringProperty( "spark.cdcloader.path.sql.basedir" ) +
      tableName
    logInfo( "Getting sql statement for " + tableName + " from " + path )
    val sqlString = sqlReader.getSQLString( sqlContext.sparkContext, path )
    logInfo( "Checking last sequence number" )
    val lastSeq = controlProcessor
      .getLastSequenceNumber( sqlContext, sqlReader, properties, tableName )
    logInfo( "last sequence was: " + lastSeq )
    logInfo( "registering temp table." )
    tableOperations.registerTempTable( data, tableName )
    logInfo( "processing " + tableName )
    val outputData = sqlContext.sql( sqlString + "'" + lastSeq + "'" )
    logInfo( "Removing temp table" )
    tableOperations.deRegisterTempTable( sqlContext, tableName )
    outputData
  }

  /**
   * Save source table dataframe to disk
   *
   * @param sqlContext the sql context
   * @param writer a dataframe writer
   * @param properties the properties object
   * @param dataFrame the dataframe to save
   * @param tableName the name of the table
   * @return the number of rows written.
   */
  def save( sqlContext : SQLContext,
            writer : DataFrameWriter,
            properties : GDHProperties,
            dataFrame : DataFrame,
            tableName : String ) : Long = {
    val path = properties.getStringProperty(
      "spark.cdcloader.path.data.outputbasedir" ) + tableName +
      properties.getArrayProperty( "spark.cdcloader.path.data.outdir" )
    logInfo( "saving " + tableName + " to " + path )
    writer.write( sqlContext,
      path,
      dataFrame,
      Some( StorageLevel.MEMORY_AND_DISK_SER ) )
  }

  /**
   * Load the source table data
   *
   * @param tableName the name of the table to load.
   * @param sqlContext the sql context
   * @param controlProcessor a control processor object
   * @param reader a dataframe reader
   * @param userFunctions a udf object
   * @param properties the properties object
   * @return a dataframe of the source table.
   */
  def load( tableName : String,
            sqlContext : SQLContext,
            controlProcessor : ControlProcessor,
            reader : DataFrameReader,
            userFunctions : UserFunctions,
            properties : GDHProperties ) : DataFrame = {

    logInfo( "Generating UDFs for " + tableName )
    val generateSequenceNumber = udf(
      () => userFunctions.generateSequenceNumber( properties ) )
    val tableNameUdf = udf(() => tableName)
    val operation = udf( () => "INSERT" )
    val timeStamp = udf( () => userFunctions.getCurrentTime( properties ) )
    val isDeleted = udf( userFunctions.isDeleted( _ : String, properties ) )
    val isAnyBitSet = udf(
      userFunctions.isAnyBitSet(
        _ : String,
        properties.getArrayProperty(
          "spark.cdcloader.control.columnpositions" + tableName ) ) )
    logInfo( "Checking if initial load. " )
    val initialLoad =
      controlProcessor.isInitialLoad( sqlContext, tableName, properties )
    //load the dataframe
    val output =
      if ( initialLoad ) {
        logInfo( "inital load was" + initialLoad + " loading base data" )
        val path = properties.getStringProperty(
          "spark.cdcloader.path.data.basedir" ) + tableName
        val chgSeqColName = properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changesequence" )
        val chgOpColName = properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" )
        logInfo( "loading data from " + path )
        logInfo( "Appending columns : " + chgSeqColName + ", " + chgOpColName )
        reader
          .read( sqlContext,
            path,
            //storage level will require tuning based on performance.
            Some( StorageLevel.MEMORY_AND_DISK_SER ) )
          .withColumn( chgSeqColName, generateSequenceNumber() )
          .withColumn( chgOpColName, operation() )
      } else {
        logInfo( "inital load was" + initialLoad + " loading change data" )
        val path = properties.getStringProperty(
          "spark.cdcloader.path.data.basedir" ) + tableName +
          properties.getStringProperty(
            "spark.cdcloader.control.attunity.changetablesuffix" )
        logInfo( "reading from " + path )
        val data = reader.read( sqlContext,
          path,
          //storage level will require tuning based on performance.
          Some( StorageLevel.MEMORY_AND_DISK_SER ) )
        if ( properties.getBooleanProperty(
          "spark.cdcloader.control.changemask.enabled" ) ) {
          logInfo( "Change mask is ACTIVE, filtering data!" )
          data.filter(
            isAnyBitSet(
              data( properties.getStringProperty(
                "spark.cdcloader.columns.attunity.name.changemask" ) ) ) )
        } else {
          logInfo( "Change mask is inactive, not filtering!" )
          data
        }
      }
    //append timestamp and deletedflag
    logInfo( "Appending metadata columns!" )
    output
      .withColumn( properties.getStringProperty(
        "spark.cdcloader.columns.metadata.name.loadtimestamp" ),
        timeStamp() )
      .withColumn( properties.getStringProperty(
        "spark.cdcloader.columns.metadata.name.isdeleted" ),
        isDeleted( output( properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" ) ) ) )
        .withColumn("_tablename", tableNameUdf())
  }

}
