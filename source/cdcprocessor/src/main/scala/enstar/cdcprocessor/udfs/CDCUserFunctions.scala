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
   * Group changes in a dataframe by transaction
   * @param df the dataframe to group
   * @param properties the GDH properties object
   * @return a dataframe filtered by group
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
    val grouped = df
      .withColumn(properties.attunityColumnPrefix + changeNumberColName,
        changeNumber(df(properties.changeSequenceColumnName)))
      .groupBy(
        df(properties.transactionIdColumnName),
        df(properties.idColumnName)
      )
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
   * Drops attunity columns from the output dataset
   * @param df the dataframe to operate on
   * @param properties the properties object
   * @return a dataframe
   */
  def dropAttunityColumns(df: DataFrame,
                          properties: CDCProperties): DataFrame =
    df.select(
      df.columns
        .filter(!_.contains(properties.attunityColumnPrefix))
        .map(col): _*)

  /**
   * Sort a dataframe by the business key
   * @param dataFrame the dataframe to sort
   * @param properties the properties object
   * @return a dataframe
   */
  def sortByKey(dataFrame: DataFrame, properties: CDCProperties): DataFrame =
    dataFrame.sort(asc(properties.idColumnName))

  /**
   * Filter before image records from the dataframe
   * @param dataFrame the dataframe to filter
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
   * Add an 'active' column to the passed in dataframe based on the value of the transaction timestamp
   * @param dataFrame the dataframe to add the column to
   * @param properties the properties object
   * @return a dataframe
   */
  def addActiveColumn(dataFrame: DataFrame,
                      properties: CDCProperties): DataFrame = {
    val setActive = udf(
      (validTo: String) => validTo.equalsIgnoreCase(activeDate))

    dataFrame.withColumn(properties.activeColumnName,
      setActive(dataFrame(properties.validToColumnName)))
  }

  /**
   * drop the active column from the passed in dataframe
   * @param dataFrame the dataframe to operate on
   * @param properties the properties object
   * @return a dataframe
   */
  def dropActiveColumn(dataFrame: DataFrame,
                       properties: CDCProperties): DataFrame =
    dataFrame.drop(properties.activeColumnName)

  /**
   * Close records that have been superseded in the dataframe
   * @param dataFrame the dataframe to process
   * @param properties the properties object
   * @return a dataframe
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
    * Filter a dataframe based on a time window, this method filters the passed in DF based on the
    * value in the transaction timestamp column.
    * @param dataFrame the dataframe to filter
    * @param properties the properties object
    * @param returnMature set to true to only return data that is older than the time window
    * @return a dataframe
    */
  def filterOnTimeWindow(dataFrame: DataFrame, properties: CDCProperties, returnMature: Boolean = true): DataFrame = {
    val filterBeforeTimeWindow = udf(
      (timeStampString: String) => {
        val currentTime = new time.DateTime().minusHours(properties.timeWindowInHours)
        val timeStamp = DateTimeFormat.forPattern(properties.attunityDateFormat).parseDateTime(timeStampString)
        timeStamp.isBefore(currentTime) == returnMature
      }
    )
    dataFrame.filter(filterBeforeTimeWindow(dataFrame(properties.transactionTimeStampColumnName)))
  }
}
