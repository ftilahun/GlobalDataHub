package com.kainos.enstar.globaldatahub.cdcloader.loader

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{ DataFrameReader, SQLFileReader }
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

class CDCLoader( controlProcessor : ControlProcessor,
                 properties : GDHProperties,
                 sqlContext : SQLContext,
                 loaderIO : DataFrameReader,
                 sqlReaderIO : SQLFileReader )
    extends Logging {

  def load = {
    controlProcessor.registerControlTable
    properties.getArrayProperty( "inputTables" ).
      foreach{ tableName =>
        //tableProcessor.processTable(tableName,sqlContext,controlProcessor,loaderIO)
      }
    controlProcessor.deregisterControlTable
  }
}
