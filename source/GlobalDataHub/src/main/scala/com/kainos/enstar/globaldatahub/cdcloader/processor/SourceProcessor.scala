package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.SQLFileReader
import com.kainos.enstar.globaldatahub.cdcloader.udfs.UserFunctions
import com.kainos.enstar.globaldatahub.common.io.{DataFrameReader, DataFrameWriter, TableOperations}
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.sql.SQLContext

/**
 * Expected behaviour for processing a source system.
 */
trait SourceProcessor {

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
               sqlReader : SQLFileReader ) : Unit

}
