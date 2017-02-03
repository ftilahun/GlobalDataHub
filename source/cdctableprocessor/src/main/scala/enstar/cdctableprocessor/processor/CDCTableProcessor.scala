package enstar.cdctableprocessor.processor

import enstar.cdctableprocessor.io.{ DataFrameReader, DataFrameWriter }
import enstar.cdctableprocessor.metrics.JobMetrics
import enstar.cdctableprocessor.properties.CDCProperties
import enstar.cdctableprocessor.udfs.UserFunctions
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
   * Write the metrics about this job to disk
   * @param metrics the metrics object to write
   * @param sqlContext the sql context
   * @param properties the properties object
   * @param writer a DataFrame Writer
   */
  def saveJobMetrics(metrics: JobMetrics,
                     properties: CDCProperties,
                     writer: DataFrameWriter)(implicit sqlContext: SQLContext): Unit = {
    val metricsDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(List(metrics)))
    writer.write(properties.metricsOutputDir.get,
      metricsDF,
      Some(StorageLevel.MEMORY_AND_DISK))
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
  override def process(properties: CDCProperties,
                       reader: DataFrameReader,
                       writer: DataFrameWriter,
                       userFunctions: UserFunctions)(implicit sqlContext: SQLContext): Unit = {

    val startTime = userFunctions.getCurrentTime(properties.attunityDateFormat)

    logInfo("Reading change data")
    val changeData = reader.read(properties.changeInputDir,
      Some(StorageLevel.MEMORY_AND_DISK_SER))
    userFunctions.persistForMetrics(changeData,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Filtering time period data")
    val immatureChanges = userFunctions
      .filterOnTimeWindow(changeData, properties, returnMature = false)

    val matureChanges =
      userFunctions.filterOnTimeWindow(changeData, properties)
    userFunctions.persistForMetrics(matureChanges,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Processing changes")
    val processedChanges =
      processChangeData(matureChanges, properties, userFunctions)
    userFunctions.persistForMetrics(processedChanges,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Reading history data")
    val history = reader.read(properties.activeInput,
      Some(StorageLevel.MEMORY_AND_DISK_SER))
    userFunctions.persistForMetrics(history,
      StorageLevel.MEMORY_ONLY,
      properties)

    /**
     * We're unable to union the history and processed change DataFrames
     * unless we remove the attunity columns.  Both the 'delete flag'
     * and 'temp timestamp' columns preserve the information we need from
     * attunity past this point.
     *
     * the delete flag defaults to false for history.
     * it is set by header_change_oper == 'D' for changes.
     */
    logInfo("Adding delete flag")
    val deleteColumnName = "kainos__isDeleted"

    val processedChangesWithDeleteFlag = userFunctions
      .addDeleteFlagColumn(processedChanges, deleteColumnName, properties)

    val historyWithDeleteFlag = userFunctions.addDeleteFlagColumn(
      history,
      deleteColumnName,
      properties,
      doNotSetFlag = true)

    /**
     * Timestamp is the transaction timestamp for changes
     * and the validFrom for history items (this should be the
     * transaction timestamp from a previous run)
     */
    logInfo("Adding temporaryTimeStamp column")
    val tempTimeStampColumnName = "kainos__tempTimeStamp"

    val processedChangesWithTimeStamp = userFunctions.addColumn(
      processedChangesWithDeleteFlag,
      tempTimeStampColumnName,
      userFunctions.getColumn(processedChangesWithDeleteFlag,
        properties.transactionTimeStampColumnName)
    )

    val historyWithTimeStamp = userFunctions.addColumn(
      historyWithDeleteFlag,
      tempTimeStampColumnName,
      userFunctions.getColumn(historyWithDeleteFlag,
        properties.validFromColumnName))

    logInfo("Dropping attunity columns")
    val processedChangesRemovedMetaData = userFunctions.dropColumn(
      userFunctions
        .dropAttunityColumns(processedChangesWithTimeStamp, properties),
      properties.activeColumnName
    )
    val historyNoMetadata =
      userFunctions.dropAttunityColumns(historyWithTimeStamp, properties)
    val historyNoActive =
      userFunctions.dropColumn(historyNoMetadata, properties.activeColumnName)

    logInfo("Joining history and changes")
    val allRecords =
      userFunctions.unionAll(processedChangesRemovedMetaData, historyNoActive)
    userFunctions.persistForMetrics(allRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Closing records")
    val openAndClosedRecords =
      userFunctions.closeRecords(allRecords,
        properties,
        deleteColumnName,
        tempTimeStampColumnName)
    userFunctions.persistForMetrics(openAndClosedRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    val flaggedRecords =
      userFunctions.addActiveColumn(openAndClosedRecords, properties)

    logInfo("Filtering active records")
    val activeRecords =
      userFunctions.filterOnActive(flaggedRecords, properties)
    userFunctions.persistForMetrics(activeRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Filtering inactive records")
    val newHistory =
      userFunctions
        .filterOnActive(flaggedRecords, properties, returnActive = false)
    userFunctions.persistForMetrics(newHistory,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Dropping delete flag")
    val newHistoryWithoutDeleteFlag =
      userFunctions.dropColumn(newHistory, deleteColumnName)
    val activeRecordsWithoutDeleteFlag =
      userFunctions.dropColumn(activeRecords, deleteColumnName)
    userFunctions.persistForMetrics(newHistoryWithoutDeleteFlag,
      StorageLevel.MEMORY_ONLY,
      properties)
    userFunctions.persistForMetrics(activeRecordsWithoutDeleteFlag,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Dropping temporary timestamp column")
    val activeRecordsWithoutTimestamp =
      userFunctions.dropColumn(activeRecordsWithoutDeleteFlag,
        tempTimeStampColumnName)
    val newHistoryWithoutTimestamp =
      userFunctions.dropColumn(newHistoryWithoutDeleteFlag,
        tempTimeStampColumnName)

    logInfo("Persisting young data to disk")
    userFunctions.countAndSave(
      properties.immatureChangesOutput,
      writer,
      immatureChanges,
      StorageLevel.MEMORY_AND_DISK_SER)

    logInfo("Saving inactive records")
    userFunctions.countAndSave(
      properties.historyOutput,
      writer,
      newHistoryWithoutTimestamp,
      StorageLevel.MEMORY_AND_DISK_SER)

    logInfo("Saving active records")
    userFunctions.countAndSave(
      properties.activeOutput,
      writer,
      activeRecordsWithoutTimestamp,
      StorageLevel.MEMORY_AND_DISK_SER)

    val completeTime = userFunctions.getCurrentTime(properties.attunityDateFormat)

    if (properties.metricsOutputDir.isDefined) {
      val metrics = JobMetrics(
        startTime = startTime,
        completeTime = completeTime,
        changeData = userFunctions.getCount(changeData),
        immatureChanges = userFunctions.getCount(immatureChanges),
        matureChanges = userFunctions.getCount(matureChanges),
        processedChanges = userFunctions.getCount(processedChanges),
        history = userFunctions.getCount(history),
        allRecords = userFunctions.getCount(allRecords),
        openAndClosedRecords = userFunctions.getCount(openAndClosedRecords),
        activeRecords = userFunctions.getCount(activeRecords),
        newHistory = userFunctions.getCount(newHistory)
      )
      logInfo(metrics.toString)
      saveJobMetrics(metrics, properties, writer)
    }
  }

}
