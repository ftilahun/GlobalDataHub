package com.kainos.enstar.globaldatahub.cdcloader.io

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Expected behaviour for a DataFrameReader
 */
trait DataFrameReader {

  /**
   * Read a DataFrame from the filesystem
   *
   * @param sqlContext the sql context.
   * @param path the path to read from
   * @param storageLevel an optional storagelevel
   * @return a dataframe
   */
  def read( sqlContext : SQLContext,
            path : String,
            storageLevel : Option[StorageLevel] ) : DataFrame

}
