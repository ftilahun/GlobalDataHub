package enstar.cdcprocessor.processor

import enstar.cdcprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdcprocessor.properties.CDCProperties
import enstar.cdcprocessor.udfs.UserFunctions
import org.apache.spark.Logging
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

/**
 * Process a source table
 */
class CDCTableProcessor extends TableProcessor with Logging {

  /**
   * Process change data  an source table
   *
   * @param changeData the sql context
   * @param properties the properties object
   * @param userFunctions a udfs object
   * @return a dataframe of the source table
   */
  def processChangeData(changeData: DataFrame,
                        properties: CDCProperties,
                        userFunctions: UserFunctions): DataFrame = {
    logInfo("Removing before image records")
    val filteredBeforeRecords = userFunctions.filterBeforeRecords(changeData, properties)

    logInfo("Getting transaction change records")
    val groupedRecords =
      userFunctions.groupByTransactionAndKey(filteredBeforeRecords, properties)

    logInfo("Closing records")
    val openAndClosedRecords = userFunctions.closeRecords(groupedRecords, properties)
    logInfo("Dropping attunity columns")
    userFunctions.dropAttunityColumns(openAndClosedRecords, properties)
  }

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
           dataFrame: DataFrame): Long = {
    writer.write(sqlContext,
      properties.activeOutput,
      dataFrame,
      Some(StorageLevel.MEMORY_AND_DISK_SER))
  }

  /**
   * Process a source table
   *
   * @param sqlContext    the sql context
   * @param properties    the properties object
   * @param reader        a dataframe reader
   * @param writer        a dataframe writer
   * @param userFunctions a udfs object
   * @return a dataframe of the source table
   */
  override def process(sqlContext: SQLContext,
                       properties: CDCProperties,
                       reader: DataFrameReader,
                       writer: DataFrameWriter,
                       userFunctions: UserFunctions): Unit = {
    logInfo("Reading change data")
    val changeData = reader.read(sqlContext,
      properties.changeInputDir,
      Some(StorageLevel.MEMORY_AND_DISK_SER))
    logInfo("Processing changes")
    val processed = processChangeData(changeData, properties, userFunctions)
    logInfo("Saving table")
    save(sqlContext, writer, properties, processed)
  }
}
