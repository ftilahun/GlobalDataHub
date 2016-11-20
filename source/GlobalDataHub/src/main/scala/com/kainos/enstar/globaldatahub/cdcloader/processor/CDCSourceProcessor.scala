package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  DataFrameReader,
  DataFrameWriter,
  SQLFileReader,
  TableOperations
}
import com.kainos.enstar.globaldatahub.cdcloader.udfs.UserFunctions
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

/**
 * Process an input source and save the output
 */
class CDCSourceProcessor extends SourceProcessor with Logging {

  /**
   * Process a source system and save to filesystem
   *
   * @param controlProcessor the control table processor for this source.
   * @param properties the properties object for this source.
   * @param sqlContext the SQLContext.
   * @param reader a dataframe reader.
   * @param writer a dataframe writer.
   * @param tableOperations object to register/deregister a control table.
   * @param tableProcessor a table processor for tables on this source.
   * @param userFunctions user defined functions for this source.
   * @param sqlReader a SQL reader for this source.
   */
  def process( controlProcessor : ControlProcessor,
               properties : GDHProperties,
               sqlContext : SQLContext,
               reader : DataFrameReader,
               writer : DataFrameWriter,
               tableOperations : TableOperations,
               tableProcessor : TableProcessor,
               userFunctions : UserFunctions,
               sqlReader : SQLFileReader ) : Unit = {
    //register the control table
    controlProcessor
      .registerControlTable( sqlContext, reader, properties, tableOperations )
    //loop over the tables.
    properties.getArrayProperty( "spark.cdcloader.input.tablenames" ).foreach {
      tableName =>
        //process each table
        val tableData = tableProcessor.process( tableName,
          sqlContext,
          controlProcessor,
          properties,
          reader,
          userFunctions,
          tableOperations,
          sqlReader )
        //save each table
        tableProcessor
          .save( sqlContext, writer, properties, tableData, tableName )
    }
    //de-register the control table
    controlProcessor
      .deregisterControlTable( sqlContext, properties, tableOperations )
  }
}
