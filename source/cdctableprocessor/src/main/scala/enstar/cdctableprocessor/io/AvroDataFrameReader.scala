package enstar.cdctableprocessor.io

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
  def read(
    path: String,
    storageLevel: Option[StorageLevel] = None)(implicit sqlContext: SQLContext): DataFrame = {
    logInfo(s"reading from path: $path")
    //include avro files without extension
    sqlContext.sparkContext.hadoopConfiguration.setBoolean("avro.mapred.ignore.inputs.without.extension", false)
    import com.kainos.spark.avro._
    val data = sqlContext.read.
      avro(new Path(path).toString)
    if (storageLevel.isDefined) {
      logInfo(
        s"Persisting DataFrame at storage level ${storageLevel.toString}")
      data.persist(storageLevel.get)
    }
    data
  }
}
