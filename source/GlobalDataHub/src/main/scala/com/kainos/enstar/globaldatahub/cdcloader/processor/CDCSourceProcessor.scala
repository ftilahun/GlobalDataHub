package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  DataFrameReader,
  TableOperations
}
import com.kainos.enstar.globaldatahub.properties.GDHProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

class CDCSourceProcessor extends Logging {

  def load( controlProcessor : ControlProcessor,
            properties : GDHProperties,
            sqlContext : SQLContext,
            reader : DataFrameReader,
            tableOperations : TableOperations ) = {
    controlProcessor
      .registerControlTable( sqlContext, reader, properties, tableOperations )
    properties.getArrayProperty( "spark.cdcloader.input.tablenames" ).foreach {
      tableName =>
      //tableProcessor.processTable( tableName, sqlContext, controlProcessor, reader )
    }
    controlProcessor
      .deregisterControlTable( sqlContext, properties, tableOperations )
  }
}
