package enstar.cdcprocessor.io

import enstar.cdcprocessor.exceptions.DataFrameReadException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * CDCDataFrameReader: reads from filesystem
 * reading from filesystem is deferred to the passed in DataFrameReader
 *
 * @param reader filesystem reader
 */
class CDCDataFrameReader(reader: DataFrameReader)
    extends DataFrameReader
    with Logging {

  /**
   * Read a DataFrame from the filesystem
   *
   * @param sqlContext the sql context.
   * @param path the path to read from
   * @param storageLevel an optional StorageLevel
   * @return a DataFrame
   */
  def read(sqlContext: SQLContext,
           path: String,
           storageLevel: Option[StorageLevel]): DataFrame = {
    try {
      logInfo("reading from " + path)
      reader.read(sqlContext, path, storageLevel)
    } catch {
      //a more readable exception
      case e: InvalidInputException =>
        logError(e.getMessage)
        throw new DataFrameReadException(path)
    }
  }

}
