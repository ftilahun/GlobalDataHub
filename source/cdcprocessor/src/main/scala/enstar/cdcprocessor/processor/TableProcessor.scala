package enstar.cdcprocessor.processor

import enstar.cdcprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.apache.spark.sql.{ DataFrame, SQLContext }

/**
 * Expected behaviour for a table processor
 */
trait TableProcessor {

  /**
   * Process an source table
   *
   * @param sqlContext    the sql context
   * @param properties    the properties object
   * @param reader        a dataframe reader
   * @param writer        a dataframe writer
   * @param userFunctions a udfs object
   * @return a dataframe of the source table
   */
  def process(sqlContext: SQLContext,
              properties: CDCProperties,
              reader: DataFrameReader,
              writer: DataFrameWriter,
              userFunctions: UserFunctions): Unit

  /**
   * Save source table dataframe to disk
   *
   * @param sqlContext the sql context
   * @param writer a dataframe writer
   * @param properties the properties object
   * @param dataFrame the dataframe to save
   * @return the number of rows written.
   */
  def save(sqlContext: SQLContext,
           writer: DataFrameWriter,
           properties: CDCProperties,
           dataFrame: DataFrame): Long

}
