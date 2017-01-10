package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.functions._
import org.joda.time
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * User functions
 */
class CDCUserFunctions extends UserFunctions {

  val activeDate = "9999-12-31 23:59:59.000"

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
    df.join(
      grouped,
      changeNumber(df(properties.changeSequenceColumnName)) === grouped(
        s"max(${properties.attunityColumnPrefix}$changeNumberColName)") &&
        df(properties.transactionIdColumnName) === grouped(
          properties.transactionIdColumnName),
      "inner")

  }

  /**
   * Drops attunity columns from the output data set
   * @param df the DataFrame to operate on
   * @param properties the properties object
   * @return a DataFrame
   */
  def dropAttunityColumns(df: DataFrame,
                          properties: CDCProperties): DataFrame =
    df.select(
      df.columns
        .filter(!_.contains(properties.attunityColumnPrefix))
        .map(col): _*)

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
   * Update the valid to date for a given row
   * @param changeOperation the change operation value
   * @param validTo the valid from date of the subsequent row
   * @param transactionTimeStamp the transaction timestamp
   * @param properties the properties object
   * @return the valid to date for this row (as a string)
   */
  def updateClosedRecord(changeOperation: String,
                         validTo: String,
                         transactionTimeStamp: String,
                         properties: CDCProperties): String = {
    if (changeOperation.equalsIgnoreCase(
      properties.operationColumnValueDelete)) {
      transactionTimeStamp
    } else if (validTo != null) {
      validTo
    } else {
      activeDate
    }
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

    dataFrame.withColumn(properties.activeColumnName,
      setActive(dataFrame(properties.validToColumnName)))
  }

  /**
   * drop the active column from the passed in DataFrame
   * @param dataFrame the DataFrame to operate on
   * @param properties the properties object
   * @return a DataFrame
   */
  def dropActiveColumn(dataFrame: DataFrame,
                       properties: CDCProperties): DataFrame =
    dataFrame.drop(properties.activeColumnName)

  /**
   * Close records that have been superseded in the DataFrame
   * @param dataFrame the DataFrame to process
   * @param properties the properties object
   * @return a DataFrame
   */
  def closeRecords(dataFrame: DataFrame,
                   properties: CDCProperties): DataFrame = {

    val byBusinessKey = Window.partitionBy(properties.idColumnName)
    val setValidTo = udf(
      (changeOp: String, validTo: String, transactionTimeStamp: String) =>
        updateClosedRecord(changeOp, validTo, transactionTimeStamp, properties)
    )
    dataFrame
      .withColumnRenamed(properties.validToColumnName,
        properties.validToColumnName + "old")
      .withColumn(properties.validToColumnName,
        setValidTo(
          col(properties.operationColumnName),
          lead(properties.validFromColumnName, 1) over byBusinessKey,
          col(properties.transactionTimeStampColumnName)
        ))
      .drop(properties.validToColumnName + "old")
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
        val currentTime =
          new time.DateTime().minusHours(properties.timeWindowInHours)
        val timeStamp = DateTimeFormat
          .forPattern(properties.attunityDateFormat)
          .parseDateTime(timeStampString)
        timeStamp.isBefore(currentTime) == returnMature
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
  override def unionAll(df1: DataFrame, df2: DataFrame): DataFrame =
    df1.unionAll(df2)
}
