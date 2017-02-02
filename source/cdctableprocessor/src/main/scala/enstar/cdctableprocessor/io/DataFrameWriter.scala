package enstar.cdctableprocessor.io

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * defines expected behaviour for a DataFrameWriter
 */
trait DataFrameWriter {

  /**
   * write a DataFrame to disk
   *
   * @param sqlContext   the hive context
   * @param path         the HDFS path to write to
   * @param data         the DataFrame
   * @param storageLevel an optional StorageLevel to persist the DataFrame
   */
  def write(path: String,
            data: DataFrame,
            storageLevel: Option[StorageLevel])(implicit sqlContext: SQLContext): Long
}
