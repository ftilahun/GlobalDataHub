package com.kainos.enstar.globaldatahub.common.io

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * defines expected behaviour for a DataFrameWriter
 */
trait DataFrameWriter {

  /**
   * write a dataframe to disk
   *
   * @param sqlContext   the hive context
   * @param path         the HDFS path to write to
   * @param data         the dataframe
   * @param storageLevel an optional storagelevel to persist the dataframe
   */
  def write( sqlContext : SQLContext,
             path : String,
             data : DataFrame,
             storageLevel : Option[StorageLevel] ) : Boolean
}
