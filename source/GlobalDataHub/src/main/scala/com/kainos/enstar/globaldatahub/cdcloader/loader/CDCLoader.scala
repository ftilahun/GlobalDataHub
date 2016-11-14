package com.kainos.enstar.globaldatahub.cdcloader.loader

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{DataFrameReader, SQLFileReader, TableOperations}
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

class CDCLoader
    extends Logging {

  def load(
            controlProcessor : ControlProcessor,
            properties : GDHProperties,
            sqlContext : SQLContext,
            reader : DataFrameReader,
            tableOperations: TableOperations
          ) = {
    controlProcessor.registerControlTable(sqlContext,reader,properties,tableOperations)
    properties.getArrayProperty( "inputTables" ).
      foreach{ tableName =>
        //tableProcessor.processTable( tableName, sqlContext, controlProcessor, reader )
      }
    controlProcessor.deregisterControlTable(sqlContext,properties,tableOperations)
  }
}
