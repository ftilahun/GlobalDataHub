package com.kainos.enstar.globaldatahub.cdcloader.io

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Expected behaviour for a DataFrameWriter
 */
trait DataFrameWriter {

  /**
   * Write a dataframe to the filesystem.
   *
   * @param sqlContext the sql context
   * @param path the path to write to
   * @param dataFrame the dataframe to write
   * @param storageLevel a storage level to persist at
   * @return a count ot the rows written.
   */
  def write( sqlContext : SQLContext,
             path : String,
             dataFrame : DataFrame,
             storageLevel : StorageLevel ) : Long

}
