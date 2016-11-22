package com.kainos.enstar.globaldatahub.cdccontrol.io

import com.kainos.enstar.globaldatahub.common.io.DataFrameWriter
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Dataframe writer for CDCControl
 */
class ControlDataFrameWriter( writer : DataFrameWriter )
    extends DataFrameWriter
    with Logging {

  /**
   * write a dataframe to disk
   *
   * @param sqlContext   the hive context
   * @param path         the HDFS path to write to
   * @param data         the dataframe
   * @param storageLevel an optional storagelevel to persist the dataframe
   */
  override def write( sqlContext : SQLContext,
                      path : String,
                      data : DataFrame,
                      storageLevel : Option[StorageLevel] ) : Long = {
    writer.write( sqlContext, path, data, storageLevel )
  }

}
