package com.kainos.enstar.globaldatahub.cdcloader.loader

import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  CDCLoaderIO,
  CDCSQLReaderIO
}
import com.kainos.enstar.globaldatahub.cdcloader.properties.CDCProperties
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext

/**
 * Created by ciaranke on 10/11/2016.
 */
class CDCLoader( controlProcessor : ControlProcessor, properties : CDCProperties )
    extends Logging {

  def load = {
    //create the control table
    controlProcessor.registerControlTable
    //process the input
    //properties.tableList.map(processTable)
    //drop the control table
    controlProcessor.deregisterControlTable
  }
}
