package enstar.cdcprocessor.io

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
  * defines expected behaviour for a DataFrameReader
  */
trait DataFrameReader {

  /**
    * read a dataframe from hdfs
    *
    * @param sqlContext   the hive context
    * @param path         the path to read from
    * @param storageLevel an optional storagelevel to persist the dataframe
    * @return a dataframe
    */
  def read(sqlContext: SQLContext,
           path: String,
           storageLevel: Option[StorageLevel]): DataFrame

}
