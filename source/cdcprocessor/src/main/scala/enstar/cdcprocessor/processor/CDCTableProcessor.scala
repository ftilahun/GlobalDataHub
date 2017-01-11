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
    userFunctions.persistForStatistics(changeData,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Filtering time period data")
    val immatureChanges = userFunctions
      .filterOnTimeWindow(changeData, properties, returnMature = false)

    val matureChanges =
      userFunctions.filterOnTimeWindow(changeData, properties)
    userFunctions.persistForStatistics(matureChanges,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Processing changes")
    val processedChanges =
      processChangeData(matureChanges, properties, userFunctions)
    userFunctions.persistForStatistics(processedChanges,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Reading history data")
    val history = reader.read(sqlContext,
      properties.historyInput,
      Some(StorageLevel.MEMORY_AND_DISK_SER))
    userFunctions.persistForStatistics(history,
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
    userFunctions.persistForStatistics(processedChangesWithDeleteFlag,
      StorageLevel.MEMORY_ONLY,
      properties)

    val historyWithDeleteFlag = userFunctions.addDeleteFlagColumn(
      history,
      deleteColumnName,
      properties,
      doNotSetFlag = true)
    userFunctions.persistForStatistics(historyWithDeleteFlag,
      StorageLevel.MEMORY_ONLY,
      properties)

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
    userFunctions.persistForStatistics(processedChangesWithTimeStamp,
      StorageLevel.MEMORY_ONLY,
      properties)

    val historyWithTimeStamp = userFunctions.addColumn(
      historyWithDeleteFlag,
      tempTimeStampColumnName,
      userFunctions.getColumn(historyWithDeleteFlag,
        properties.validFromColumnName))
    userFunctions.persistForStatistics(historyWithTimeStamp,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Dropping attunity columns")
    val processedChangesRemovedMetaData = userFunctions
      .dropAttunityColumns(processedChangesWithTimeStamp, properties)
    val historyNoMetadata =
      userFunctions.dropAttunityColumns(historyWithTimeStamp, properties)
    val historyNoActive =
      userFunctions.dropColumn(historyNoMetadata, properties.activeColumnName)
    userFunctions.persistForStatistics(processedChangesRemovedMetaData,
      StorageLevel.MEMORY_ONLY,
      properties)
    userFunctions.persistForStatistics(historyNoMetadata,
      StorageLevel.MEMORY_ONLY,
      properties)
    userFunctions.persistForStatistics(historyNoActive,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Joining history and changes")
    val allRecords =
      userFunctions.unionAll(processedChangesRemovedMetaData, historyNoActive)
    userFunctions.persistForStatistics(allRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Closing records")
    val openAndClosedRecords =
      userFunctions.closeRecords(allRecords,
        properties,
        deleteColumnName,
        tempTimeStampColumnName)
    userFunctions.persistForStatistics(openAndClosedRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    val flaggedRecords =
      userFunctions.addActiveColumn(openAndClosedRecords, properties)
    userFunctions.persistForStatistics(flaggedRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Filtering active records")
    val activeRecords =
      userFunctions.filterOnActive(flaggedRecords, properties)
    userFunctions.persistForStatistics(activeRecords,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Filtering inactive records")
    val newHistory =
      userFunctions
        .filterOnActive(flaggedRecords, properties, returnActive = false)
    userFunctions.persistForStatistics(newHistory,
      StorageLevel.MEMORY_ONLY,
      properties)

    logInfo("Dropping delete flag")
    val newHistoryWithoutDeleteFlag =
      userFunctions.dropColumn(newHistory, deleteColumnName)
    val activeRecordsWithoutDeleteFlag =
      userFunctions.dropColumn(activeRecords, deleteColumnName)
    userFunctions.persistForStatistics(newHistoryWithoutDeleteFlag,
      StorageLevel.MEMORY_ONLY,
      properties)
    userFunctions.persistForStatistics(activeRecordsWithoutDeleteFlag,
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
    countAndSave(sqlContext,
      properties.immatureChangesOutput,
      writer,
      immatureChanges,
      StorageLevel.MEMORY_AND_DISK_SER)

    logInfo("Saving inactive records")
    countAndSave(sqlContext,
      properties.historyOutput,
      writer,
      newHistoryWithoutTimestamp,
      StorageLevel.MEMORY_AND_DISK_SER)

    logInfo("Saving active records")
    countAndSave(sqlContext,
      properties.activeOutput,
      writer,
      activeRecordsWithoutTimestamp,
      StorageLevel.MEMORY_AND_DISK_SER)

    if (properties.printStatistics) {
      val statisticsString = "==========================================================================" +
        "\nStatistics for this run" +
        "\n==========================================================================" +
        s"\nChanges read: ${userFunctions.getCount(changeData)}" +
        s"\nRejected Changes: ${userFunctions.getCount(immatureChanges)}" +
        s"\nAccepted Changes: ${userFunctions.getCount(matureChanges)}" +
        s"\nDistinct Transactions: ${userFunctions.getCount(processedChanges)}" +
        s"\n\t(with delete flag): ${userFunctions.getCount(processedChangesWithDeleteFlag)}" +
        s"\n\t(with timestamp): ${userFunctions.getCount(processedChangesWithTimeStamp)}" +
        s"\n\t(with metadata removed): ${userFunctions.getCount(processedChangesRemovedMetaData)}" +
        s"\nBase Records: ${userFunctions.getCount(history)}" +
        s"\n\t(with delete flag):${userFunctions.getCount(historyWithDeleteFlag)}" +
        s"\n\t(with timestamp): ${userFunctions.getCount(historyWithTimeStamp)}" +
        s"\n\t(with metadata removed): ${userFunctions.getCount(historyNoMetadata)}" +
        s"\n\t(with active removed): ${userFunctions.getCount(historyNoActive)}" +
        s"\nTotal Records (unioned): ${userFunctions.getCount(allRecords)}" +
        s"\n\t(After setting validTo): ${userFunctions.getCount(openAndClosedRecords)}" +
        s"\n\t(After Adding active flag): ${userFunctions.getCount(flaggedRecords)}" +
        s"\nActive Records: ${userFunctions.getCount(activeRecords)}" +
        s"\n\t(with delete flag removed): ${userFunctions.getCount(activeRecordsWithoutDeleteFlag)}" +
        s"\n\t(with timestamp removed): ${userFunctions.getCount(activeRecordsWithoutTimestamp)}" +
        s"\nInactive Records: ${userFunctions.getCount(newHistory)}" +
        s"\n\t(with delete flag removed): ${userFunctions.getCount(newHistoryWithoutDeleteFlag)}" +
        s"\n\t(with timestamp removed): ${userFunctions.getCount(newHistoryWithoutTimestamp)}" +
        "\n=========================================================================="
      logInfo(statisticsString)
    }

  }

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
                   storageLevel: StorageLevel): Unit = {
    dataFrame.persist(storageLevel)
    if (dataFrame.count() > 0) {
      writer.write(sqlContext, path, dataFrame, Some(storageLevel))
    }
  }

}
