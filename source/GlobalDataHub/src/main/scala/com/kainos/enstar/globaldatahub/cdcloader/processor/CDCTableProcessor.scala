package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  SQLFileReader,
  DataFrameReader,
  DataFrameWriter,
  TableOperations
}
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import com.kainos.enstar.globaldatahub.cdcloader.udfs.UserFunctions
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * Process a source table
 */
class CDCTableProcessor extends TableProcessor {

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
               sqlReader : SQLFileReader ) : DataFrame = {

    //get the dataframe for source or initital load
    val data = load( tableName,
      sqlContext,
      controlProcessor,
      reader,
      userFunctiions,
      properties )
    //get the sql statement to process the dataframe with
    val sqlString = sqlReader.getSQLString(
      sqlContext.sparkContext,
      properties.getStringProperty( "spark.cdcloader.paths.sql.basedir" ) +
        tableName )
    //get the last sequence number from the control table
    val lastSeq = controlProcessor
      .getLastSequenceNumber( sqlContext, sqlReader, properties, tableName )
    //register a temp table and process
    tableOperations.registerTempTable( data, tableName )
    val outputData = sqlContext.sql( sqlString + "'" + lastSeq + "'" )
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
    writer.write( sqlContext,
      properties.getStringProperty(
        "spark.cdcloader.paths.data.outputbasedir" ) + tableName +
        properties.getArrayProperty(
          "spark.cdcloader.paths.data.outdir" ),
      dataFrame,
      StorageLevel.MEMORY_AND_DISK_SER )
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

    //create required udfs
    val generateSequenceNumber = udf(
      () => userFunctions.generateSequenceNumber( properties ) )
    val operation = udf( () => "INSERT" )
    val timeStamp = udf( () => userFunctions.getCurrentTime( properties ) )
    val isDeleted = udf( userFunctions.isDeleted( _ : String, properties ) )
    val isAnyBitSet = udf(
      userFunctions.isAnyBitSet(
        _ : String,
        properties.getArrayProperty(
          "spark.cdcloader.control.columnpositions" + tableName ) ) )

    //load the dataframe
    val output =
      if ( controlProcessor.isInitialLoad( sqlContext, tableName, properties ) ) {
        //load initial data
        reader
          .read( sqlContext,
            properties.getStringProperty(
              "spark.cdcloader.paths.data.basedir" ) + tableName,
            //storage level will require tuning based on performance.
            Some( StorageLevel.MEMORY_AND_DISK_SER ) )
          .withColumn(
            properties.getStringProperty(
              "spark.cdcloader.columns.attunity.name.changesequence" ),
            generateSequenceNumber() )
          .withColumn(
            properties.getStringProperty(
              "spark.cdcloader.columns.attunity.name.changeoperation" ),
            operation() )
      } else {
        //load change data
        val data = reader
          .read( sqlContext,
            properties.getStringProperty(
              "spark.cdcloader.paths.data.basedir" ) + tableName +
              properties.getStringProperty(
                "spark.cdcloader.control.attunity.changetablesuffix" ),
            //storage level will require tuning based on performance.
            Some( StorageLevel.MEMORY_AND_DISK_SER ) )

        if ( properties.getBooleanProperty(
          "spark.cdcloader.control.changemask.enabled" ) ) {
          data.filter(
            isAnyBitSet(
              data( properties.getStringProperty(
                "spark.cdcloader.columns.attunity.name.changemask" ) ) ) )
        } else {
          data
        }
      }
    //append timestamp and deletedflag
    output
      .withColumn( properties.getStringProperty(
        "spark.cdcloader.columns.metadata.name.loadtimestamp" ),
        timeStamp() )
      .withColumn( properties.getStringProperty(
        "spark.cdcloader.columns.metadata.name.isdeleted" ),
        isDeleted( output( properties.getStringProperty(
          "spark.cdcloader.columns.attunity.name.changeoperation" ) ) ) )
  }

}
