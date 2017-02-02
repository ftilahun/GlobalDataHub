package enstar.cdctableprocessor.processor

import enstar.cdctableprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdctableprocessor.properties.CDCProperties
import enstar.cdctableprocessor.udfs.UserFunctions
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
  def process(
    properties: CDCProperties,
    reader: DataFrameReader,
    writer: DataFrameWriter,
    userFunctions: UserFunctions)(implicit sqlContext: SQLContext): Unit

}
