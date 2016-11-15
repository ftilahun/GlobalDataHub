package com.kainos.enstar.globaldatahub.io

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel
import com.databricks.spark.avro._

/**
 * Helper class for writing dataframes to HDFS
 */
class GDHDataFrameWriter extends Logging with DataFrameWriter {

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
             storageLevel : Option[StorageLevel] ) : Boolean = {
    if ( storageLevel.isDefined ) {
      logInfo(
        s"Persisting dataframe at storage level ${storageLevel.toString}" )
      data.persist( storageLevel.get )
    }
    logInfo( s"Saving to path: $path" )
    data.write.avro( new Path( path ).toString )
    true
  }

}
