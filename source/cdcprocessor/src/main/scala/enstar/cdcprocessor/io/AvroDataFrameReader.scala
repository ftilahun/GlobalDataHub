package enstar.cdcprocessor.io

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Helper class for reading DataFrames from HDFS
 */
class AvroDataFrameReader extends Logging with DataFrameReader {

  /**
   * read a DataFrame from HDFS
   *
   * @param sqlContext   the hive context
   * @param path         the path to read from
   * @param storageLevel an optional StorageLevel to persist the DataFrame
   * @return a DataFrame
   */
  def read(sqlContext: SQLContext,
           path: String,
           storageLevel: Option[StorageLevel]): DataFrame = {
    logInfo(s"reading from path: $path")
    import com.databricks.spark.avro._
    val data = sqlContext.read.avro(new Path(path).toString)

    if (storageLevel.isDefined) {
      logInfo(
        s"Persisting DataFrame at storage level ${storageLevel.toString}")
      data.persist(storageLevel.get)
    }
    data
  }
}
