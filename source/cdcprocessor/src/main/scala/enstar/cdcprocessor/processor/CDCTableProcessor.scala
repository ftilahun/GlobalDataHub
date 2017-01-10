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
   * @param userFunctions a UDFs object
   * @return a DataFrame of the source table
   */
  def processChangeData(changeData: DataFrame,
                        properties: CDCProperties,
                        userFunctions: UserFunctions): DataFrame = {
    logInfo("Removing before image records")
    val filteredBeforeRecords =
      userFunctions.filterBeforeRecords(changeData, properties)

    logInfo("Getting transaction change records")
    userFunctions.groupByTransactionAndKey(filteredBeforeRecords, properties)
  }

  /**
   * Process a source table
   *
   * @param sqlContext    the sql context
   * @param properties    the properties object
   * @param reader        a DataFrame reader
   * @param writer        a DataFrame writer
   * @param userFunctions a UDFs object
   * @return a DataFrame of the source table
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

    logInfo("Filtering time period data")
    val immatureChanges = userFunctions
      .filterOnTimeWindow(changeData, properties, returnMature = false)
    val matureChanges =
      userFunctions.filterOnTimeWindow(changeData, properties)

    logInfo("Processing changes")
    val processedChanges = processChangeData(matureChanges, properties, userFunctions)

    logInfo("Reading history data")
    val history = reader.read(sqlContext,
      properties.historyInput,
      Some(StorageLevel.MEMORY_AND_DISK_SER))

    logInfo("Joining history and changes")
    val allRecords = userFunctions.unionAll(processedChanges, history)

    logInfo("Closing records")
    val openAndClosedRecords =
      userFunctions.closeRecords(allRecords, properties)

    logInfo("Dropping attunity columns")
    val removedMetaData = userFunctions.dropAttunityColumns(openAndClosedRecords, properties)

    logInfo("Filtering active records")
    val activeRecords = userFunctions.filterOnActive(removedMetaData, properties)

    logInfo("Filtering inactive records")
    val newHistory =
      userFunctions
        .filterOnActive(allRecords, properties, returnActive = false)

    logInfo("Persisting young data to disk")
    writer.write(sqlContext,
      properties.immatureChangesOutput,
      immatureChanges,
      Some(StorageLevel.MEMORY_AND_DISK_SER))

    logInfo("Saving inactive records")
    writer.write(sqlContext,
      properties.historyOutput,
      newHistory,
      Some(StorageLevel.MEMORY_AND_DISK_SER))

    logInfo("Saving active records")
    writer.write(sqlContext,
      properties.activeOutput,
      activeRecords,
      Some(StorageLevel.MEMORY_AND_DISK_SER))
  }
}
