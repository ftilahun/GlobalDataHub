package com.kainos.enstar.globaldatahub.cdcloader.loader

import com.kainos.enstar.globaldatahub.cdcloader.control.CDCControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.CDCDataFrameReader
import org.apache.spark.sql.SQLContext

class TableProcessor {

  def processTable(
    tableName : String,
    sqlContext : SQLContext,
    controlProcessor : CDCControlProcessor,
    loaderIO : CDCDataFrameReader ) = {
    if ( controlProcessor.isInitialLoad( sqlContext, tableName ) ) {
      //loaderIO.read(sqlContext,)
    } else {

    }

  }

}
