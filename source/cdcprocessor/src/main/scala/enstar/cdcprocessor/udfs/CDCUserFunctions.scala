package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * User functions
 */
class CDCUserFunctions extends UserFunctions with Logging {

  val activeDate = "9999-12-31 23:59:59.000"
  val currentTime = new DateTime()

  /**
   * * Provides a string representation of the current time in the specified
   * format
   * @param format the string format
   * @return a string representation of the current timestamp
   */
  def getCurrentTime(format: String): String =
    DateTimeFormat.forPattern(format).print(new DateTime())

  /**
   * Group changes in a DataFrame by transaction
   * @param df the DataFrame to group
   * @param properties the GDH properties object
   * @return a DataFrame filtered by group
   */
  def groupByTransactionAndKey(df: DataFrame,
                               properties: CDCProperties): DataFrame = {
    val changeNumberColName = "changeNumber"
    val changeNumber = udf(
      (s: String) => {
        //the length of the date string in the attunity change sequence
        val changeSeqDateLength = 16
        s.substring(changeSeqDateLength).toLong
      }
    )

    val colList = properties.idColumnName
      .split(",")
      .map(df(_))
      .toList
      .::(df(properties.transactionIdColumnName))
    val grouped = df
      .withColumn(properties.attunityColumnPrefix + changeNumberColName,
        changeNumber(df(properties.changeSequenceColumnName)))
      .groupBy(colList: _*)
      .max(properties.attunityColumnPrefix + changeNumberColName)
      .select(
        s"max(${properties.attunityColumnPrefix}$changeNumberColName)",
        properties.transactionIdColumnName
      )
    df.join(
      grouped,
      changeNumber(df(properties.changeSequenceColumnName)) === grouped(
        s"max(${properties.attunityColumnPrefix}$changeNumberColName)") &&
        df(properties.transactionIdColumnName) === grouped(
          properties.transactionIdColumnName),
      "inner"
    )
  }

  /**
   * Drops attunity columns from the output data set
   * @param df the DataFrame to operate on
   * @param properties the properties object
   * @return a DataFrame
   */
  def dropAttunityColumns(df: DataFrame,
                          properties: CDCProperties): DataFrame = {
    logInfo(s"Dropping columns with prefix ${properties.attunityColumnPrefix}")
    logInfo(s"pre: ${df.columns.mkString(",")}")
    val result = df.select(
      df.columns
        .filter(!_.contains(properties.attunityColumnPrefix))
        .map(col): _*)
    logInfo(s"post: ${result.columns.mkString(",")}")
    result
  }

  /**
   * Sort a DataFrame by the business key
   * @param dataFrame the DataFrame to sort
   * @param properties the properties object
   * @return a DataFrame
   */
  def sortByKey(dataFrame: DataFrame, properties: CDCProperties): DataFrame =
    dataFrame.sort(asc(properties.idColumnName))

  /**
   * Filter before image records from the DataFrame
   * @param dataFrame the DataFrame to filter
   * @param properties the properties object
   * @return
   */
  def filterBeforeRecords(dataFrame: DataFrame,
                          properties: CDCProperties): DataFrame = {
    dataFrame.filter(
      !col(properties.operationColumnName)
        .contains(properties.operationColumnValueBefore))
  }

  /**
   * Add an 'active' column to the passed in DataFrame based on the value of the transaction timestamp
   * @param dataFrame the DataFrame to add the column to
   * @param properties the properties object
   * @return a DataFrame
   */
  def addActiveColumn(dataFrame: DataFrame,
                      properties: CDCProperties): DataFrame = {
    val setActive = udf(
      (validTo: String) => validTo.equalsIgnoreCase(activeDate))
    addColumn(dataFrame,
      properties.activeColumnName,
      setActive(dataFrame(properties.validToColumnName)))
  }

  /**
   * Close records that have been superseded in the DataFrame
   * @param dataFrame the DataFrame to process
   * @param properties the properties object
   * @param deleteFlagColumnName: The name of the deleteFlag column
   * @param timestampColumnName name The name of the field containing the transaction timestamp
   * @return a DataFrame
   */
  def closeRecords(dataFrame: DataFrame,
                   properties: CDCProperties,
                   deleteFlagColumnName: String,
                   timestampColumnName: String): DataFrame = {

    val byBusinessKey = Window.partitionBy(
      properties.idColumnName.split(",").map(dataFrame(_)).toSeq: _*)
    val setValidTo = udf((isDeleted: Boolean, validTo: String,
      transactionTimeStamp: String) =>
      updateClosedRecord(isDeleted, validTo, transactionTimeStamp, properties))

    dataFrame
      .withColumnRenamed(properties.validToColumnName,
        properties.validToColumnName + "old")
      .withColumn(properties.validToColumnName,
        setValidTo(
          col(deleteFlagColumnName),
          lead(properties.validFromColumnName, 1) over byBusinessKey,
          col(timestampColumnName)
        ))
      .drop(properties.validToColumnName + "old")
  }

  /**
   * Update the valid to date for a given row
   * @param isDeleted flag indicating if the record has been deleted
   * @param validTo the valid from date of the subsequent row
   * @param transactionTimeStamp the transaction timestamp
   * @param properties the properties object
   * @return the valid to date for this row (as a string)
   */
  def updateClosedRecord(isDeleted: Boolean,
                         validTo: String,
                         transactionTimeStamp: String,
                         properties: CDCProperties): String = {
    if (isDeleted) {
      transactionTimeStamp
    } else if (validTo != null) {
      validTo
    } else {
      activeDate
    }
  }

  /**
   * Filter a DataFrame based on a time window, this method filters the passed in DF based on the
   * value in the transaction timestamp column.
   * @param dataFrame the DataFrame to filter
   * @param properties the properties object
   * @param returnMature set to true to only return data that is older than the time window
   * @return a DataFrame
   */
  def filterOnTimeWindow(dataFrame: DataFrame,
                         properties: CDCProperties,
                         returnMature: Boolean = true): DataFrame = {
    val filterBeforeTimeWindow = udf(
      (timeStampString: String) => {
        val cutOff =
          currentTime.minusHours(properties.timeWindowInHours)
        val timeStamp = try {
          DateTimeFormat
            .forPattern(properties.attunityDateFormat)
            .parseDateTime(timeStampString)
        } catch {
          case _: IllegalArgumentException =>
            DateTimeFormat
              .forPattern(properties.attunityDateFormatShort)
              .parseDateTime(timeStampString)
        }
        timeStamp.isBefore(cutOff) == returnMature
      }
    )
    dataFrame.filter(
      filterBeforeTimeWindow(
        dataFrame(properties.transactionTimeStampColumnName)))
  }

  /**
   * Filter a DataFrame based on the value of the 'active' column
   * @param allRecords the DataFrame to filter
   * @param properties the properties object
   * @param returnActive set to false to return records where active=false
   * @return a DataFrame
   */
  def filterOnActive(allRecords: DataFrame,
                     properties: CDCProperties,
                     returnActive: Boolean = true): DataFrame =
    allRecords.filter(allRecords(properties.activeColumnName) === returnActive)

  /**
   * Join two DataFrames
   *
   * @param df1 the DataFrame to join to
   * @param df2 the DataFrame to Join
   * @return a DataFrame
   */
  override def unionAll(df1: DataFrame, df2: DataFrame): DataFrame = {
    logInfo(
      s"union (${df1.columns.mkString(",")}) with (${df1.columns.mkString(",")})")
    df1.unionAll(df2)
  }

  /**
   * Adds a column to the DataFrame indicating wether or not the record should be deleted
   * @param dataFrame the DataFrame to add the column to
   * @param columnName the name of the column
   * @param properties the properties object
   * @param doNotSetFlag an override to set all rows to false
   * @return a dataframe
   */
  def addDeleteFlagColumn(dataFrame: DataFrame,
                          columnName: String,
                          properties: CDCProperties,
                          doNotSetFlag: Boolean = false): DataFrame = {
    val returnFalse = udf(() => false)
    val setDeleteFlag = udf((changeOperation: String) =>
      changeOperation.equalsIgnoreCase(properties.operationColumnValueDelete))
    addColumn(dataFrame, columnName, if (doNotSetFlag) {
      returnFalse()
    } else {
      setDeleteFlag(dataFrame(properties.operationColumnName))
    })
  }

  /**
   * Drops a column from the DataFrame
   * @param dataFrame the DataFrame to operate on
   * @param columnName the name of the column to drop
   * @return A DataFrame
   */
  def dropColumn(dataFrame: DataFrame, columnName: String): DataFrame = {
    dataFrame.drop(columnName)
  }

  /**
   * Add a column to a DataFrame
   *
   * @param dataFrame  the DataFrame to add to
   * @param columnName the name of the new column
   * @param column     the column to add
   * @return a DataFrame
   */
  override def addColumn(dataFrame: DataFrame,
                         columnName: String,
                         column: Column): DataFrame = {
    dataFrame.withColumn(columnName, column)
  }

  /**
   * Return a specific column from a DataFrame
   *
   * @param dataFrame  the DataFrame to extract from
   * @param columnName the name of the column to extract
   * @return a Column
   */
  override def getColumn(dataFrame: DataFrame, columnName: String): Column =
    dataFrame(columnName)

  /**
   * Persist a DataFrame at the specified storage level
   * @param dataFrame the DataFrame to persist
   * @param storageLevel the StorageLevel to persist at
   * @return
   */
  def persistForStatistics(dataFrame: DataFrame,
                           storageLevel: StorageLevel,
                           properties: CDCProperties): Unit = {
    if (properties.printStatistics) {
      dataFrame.persist(storageLevel)
    }
  }

  /**
   * Get a count of the rows in a DataFrame
   *
   * @param dataFrame the DataFrame to count
   * @return the number of rows
   */
  override def getCount(dataFrame: DataFrame): Long = dataFrame.count()
}
