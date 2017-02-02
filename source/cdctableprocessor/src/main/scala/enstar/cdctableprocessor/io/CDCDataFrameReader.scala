package enstar.cdctableprocessor.io

import enstar.cdctableprocessor.exceptions.DataFrameReadException
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
  def read(
    path: String,
    storageLevel: Option[StorageLevel])(implicit sqlContext: SQLContext): DataFrame = {
    try {
      logInfo("reading from " + path)
      reader.read(path, storageLevel)
    } catch {
      //a more readable exception
      case e: InvalidInputException =>
        if (e != null) {
          logError(e.getMessage)
        }
        throw new DataFrameReadException(path)
    }
  }

}
