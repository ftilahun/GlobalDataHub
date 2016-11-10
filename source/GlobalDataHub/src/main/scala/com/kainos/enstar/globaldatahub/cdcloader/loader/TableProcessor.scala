package com.kainos.enstar.globaldatahub.cdcloader.loader

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  CDCLoaderIO,
  CDCSQLReaderIO
}
import com.kainos.enstar.globaldatahub.cdcloader.properties.CDCProperties
import org.apache.spark.sql.SQLContext

/**
 * Created by ciaranke on 10/11/2016.
 */
class TableProcessor( sqlContext : SQLContext,
                      loaderIO : CDCLoaderIO,
                      sqlReaderIO : CDCSQLReaderIO,
                      controlProcessor : ControlProcessor,
                      properties : CDCProperties ) {

  def processTable( tableName : String ) = {
    val tableDF = loaderIO.read(
      sqlContext,
      properties.getStringProperty( "basePath" ) + tableName,
      None )
    loaderIO.registerTempTable( tableDF, tableName )
    val lastSequence = controlProcessor.getLastSequenceNumber( tableName )

    if ( properties.getBoolenProperty( "filterColumns" ) ) {}

    loaderIO.deRegisterTempTable( sqlContext, tableName )
  }

}
