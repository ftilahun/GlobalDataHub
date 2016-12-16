package enstar.globaldatahub.io

import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
  * Helper class for writing dataframes to HDFS
  */
class AvroDataFrameWriter extends Logging with DataFrameWriter {

  /**
    * write a dataframe to disk
    *
    * @param sqlContext   the hive context
    * @param path         the HDFS path to write to
    * @param data         the dataframe
    * @param storageLevel an optional storagelevel to persist the dataframe
    */
  def write(sqlContext: SQLContext,
            path: String,
            data: DataFrame,
            storageLevel: Option[StorageLevel]): Long = {
    if (storageLevel.isDefined) {
      logInfo(
        s"Persisting dataframe at storage level ${storageLevel.toString}")
      data.persist(storageLevel.get)
    }
    logInfo(s"Saving to path: $path")
    import com.databricks.spark.avro._
    data.write.avro(new Path(path).toString)
    data.count
  }

}
