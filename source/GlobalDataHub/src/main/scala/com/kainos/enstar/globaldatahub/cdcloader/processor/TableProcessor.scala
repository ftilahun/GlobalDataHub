package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  SQLFileReader,
  DataFrameReader,
  DataFrameWriter,
  TableOperations
}
import com.kainos.enstar.globaldatahub.cdcloader.udfs.UserFunctions
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.sql.{ DataFrame, SQLContext }

trait TableProcessor {

  /** Process an source table
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
  def process(tableName: String,
              sqlContext: SQLContext,
              controlProcessor: ControlProcessor,
              properties: GDHProperties,
              reader: DataFrameReader,
              userFunctiions: UserFunctions,
              tableOperations: TableOperations,
              sqlReader: SQLFileReader): DataFrame

  /** Save source table dataframe to disk
    *
    * @param sqlContext the sql context
    * @param writer a dataframe writer
    * @param properties the properties object
    * @param dataFrame the dataframe to save
    * @param tableName the name of the table
    * @return the number of rows written.
    */
  def save(sqlContext: SQLContext,
           writer: DataFrameWriter,
           properties: GDHProperties,
           dataFrame: DataFrame,
           tableName: String): Long

  /** Load the source table data
    *
    * @param tableName the name of the table to load.
    * @param sqlContext the sql context
    * @param controlProcessor a control processor object
    * @param reader a dataframe reader
    * @param userFunctions a udf object
    * @param properties the properties object
    * @return a dataframe of the source table.
    */
  def load(tableName: String,
           sqlContext: SQLContext,
           controlProcessor: ControlProcessor,
           reader: DataFrameReader,
           userFunctions: UserFunctions,
           properties: GDHProperties): DataFrame
}
