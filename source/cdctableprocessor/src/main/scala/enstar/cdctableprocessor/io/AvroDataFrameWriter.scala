package enstar.cdctableprocessor.io

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Helper class for writing DataFrames to HDFS
 */
class AvroDataFrameWriter extends Logging with DataFrameWriter {

  /**
   * write a DataFrame to disk
   *
   * @param sqlContext   the hive context
   * @param path         the HDFS path to write to
   * @param data         the DataFrame
   * @param storageLevel an optional StorageLevel to persist the DataFrame
   */
  def write(
    path: String,
    data: DataFrame,
    storageLevel: Option[StorageLevel])(implicit sqlContext: SQLContext): Long = {

    if (storageLevel.isDefined) {
      logInfo(
        s"Persisting DataFrame at storage level ${storageLevel.toString}")
      data.persist(storageLevel.get)
    }

    logInfo(s"Saving to path: $path")
    import com.kainos.spark.avro._
    data.write.avro(new Path(path).toString)
    data.count
  }

}
