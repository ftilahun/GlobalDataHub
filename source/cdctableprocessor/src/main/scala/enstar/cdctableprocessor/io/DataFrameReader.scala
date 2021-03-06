package enstar.cdctableprocessor.io

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * defines expected behaviour for a DataFrameReader
 */
trait DataFrameReader {

  /**
   * read a DataFrame from hdfs
   *
   * @param sqlContext   the hive context
   * @param path         the path to read from
   * @param storageLevel an optional StorageLevel to persist the DataFrame
   * @return a DataFrame
   */
  def read(path: String,
           storageLevel: Option[StorageLevel] = None)(implicit sqlContext: SQLContext): DataFrame

}
