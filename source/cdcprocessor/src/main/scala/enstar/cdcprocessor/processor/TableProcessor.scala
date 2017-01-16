package enstar.cdcprocessor.processor

import enstar.cdcprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.apache.spark.sql.SQLContext

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

}
