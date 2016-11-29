package enstar.globaldatahub.cdcloader.io

import enstar.globaldatahub.common.exceptions.DataFrameWriteException
import enstar.globaldatahub.common.io.DataFrameWriter
import org.apache.hadoop.fs.PathExistsException
import org.apache.spark.Logging
import org.apache.spark.sql.{ AnalysisException, DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Writes a DataFrame to filesystem.
 * @param writer filesystem writer
 */
class CDCDataFrameWriter(writer: DataFrameWriter)
    extends DataFrameWriter
    with Logging {

  /**
   * Write a dataframe to the filesystem.
   *
   * @param sqlContext the sql context
   * @param path the path to write to
   * @param dataFrame the dataframe to write
   * @param storageLevel a storage level to persist at
   * @return a count ot the rows written.
   */
  def write(sqlContext: SQLContext,
            path: String,
            dataFrame: DataFrame,
            storageLevel: Option[StorageLevel]): Long = {
    try {
      logInfo("Writing to: " + path)
      writer.write(sqlContext, path, dataFrame, storageLevel)
      val count = dataFrame.count
      logInfo("Wrote " + count + " rows.")
      count
    } catch {
      //a more readable exception
      case e: AnalysisException =>
        throw new DataFrameWriteException(path, e)
    }
  }
}
