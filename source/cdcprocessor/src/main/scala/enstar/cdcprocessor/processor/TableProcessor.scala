package enstar.cdcprocessor.processor

import enstar.cdcprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Expected behaviour for a table processor
 */
trait TableProcessor {

  /**
   * Process an source table
   *
   * @param sqlContext    the sql context
   * @param properties    the properties object
   * @param reader        a DataFrame reader
   * @param writer        a DataFrame writer
   * @param userFunctions a UDFs object
   * @return a DataFrame of the source table
   */
  def process(sqlContext: SQLContext,
              properties: CDCProperties,
              reader: DataFrameReader,
              writer: DataFrameWriter,
              userFunctions: UserFunctions): Unit

  /**
   * Count the number of records in a DataFrame and if >0 save it to disk
   * @param sqlContext the sql context
   * @param path the path to save to
   * @param writer a DataFrame writer
   * @param dataFrame the DataFrame to save
   * @param storageLevel the storage level to persist at
   */
  def countAndSave(sqlContext: SQLContext,
                   path: String,
                   writer: DataFrameWriter,
                   dataFrame: DataFrame,
                   storageLevel: StorageLevel): Unit

}
