package enstar.globaldatahub.cdccontrol.io

import enstar.globaldatahub.common.io.DataFrameReader
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Dataframe reader for CDCControl
 */
class ControlDataFrameReader(reader: DataFrameReader)
    extends DataFrameReader
    with Logging {

  /**
   * read a dataframe from hdfs
   *
   * @param sqlContext   the hive context
   * @param path         the path to read from
   * @param storageLevel an optional storagelevel to persist the dataframe
   * @return a dataframe
   */
  override def read(sqlContext: SQLContext,
                    path: String,
                    storageLevel: Option[StorageLevel]): DataFrame = {
    reader.read(sqlContext, path, storageLevel)
  }

}
