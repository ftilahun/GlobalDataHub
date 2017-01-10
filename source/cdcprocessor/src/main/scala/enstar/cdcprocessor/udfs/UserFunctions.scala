package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.DataFrame

trait UserFunctions extends Serializable {

  /**
   * Join two DataFrames
   * @param df1 the DataFrame to join to
   * @param df2 the DataFrame to Join
   * @return a DataFrame
   */
  def unionAll(df1: DataFrame, df2: DataFrame): DataFrame

  /**
   * * Provides a string representation of the current time in the specified
   * format
   *
   * @param format the string format
   * @return a string representation of the current timestamp
   */
  def getCurrentTime(format: String): String

  /**
   * Group changes in a DataFrame by transaction
   * @param df the DataFrame to group
   * @param properties the GDH properties object
   * @return a DataFrame filtered by group
   */
  def groupByTransactionAndKey(df: DataFrame,
                               properties: CDCProperties): DataFrame

  /**
   * Drops attunity columns from the output data set
   * @param df the DataFrame to operate on
   * @param properties the properties object
   * @return a DataFrame
   */
  def dropAttunityColumns(df: DataFrame, properties: CDCProperties): DataFrame

  /**
   * Filter before image records from the DataFrame
   * @param dataFrame the DataFrame to filter
   * @param properties the properties object
   * @return
   */
  def filterBeforeRecords(dataFrame: DataFrame,
                          properties: CDCProperties): DataFrame

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
                         properties: CDCProperties): String

  /**
   * Close records that have been superseded in the DataFrame
   * @param dataFrame the DataFrame to process
   * @param properties the properties object
   * @return a DataFrame
   */
  def closeRecords(dataFrame: DataFrame, properties: CDCProperties): DataFrame

  /**
   * Add an 'active' column to the passed in DataFrame based on the value of the transaction timestamp
   * @param dataFrame the DataFrame to add the column to
   * @param properties the properties object
   * @return a DataFrame
   */
  def addActiveColumn(dataFrame: DataFrame,
                      properties: CDCProperties): DataFrame

  /**
   * drop the active column from the passed in DataFrame
   * @param dataFrame the DataFrame to operate on
   * @param properties the properties object
   * @return a DataFrame
   */
  def dropActiveColumn(dataFrame: DataFrame,
                       properties: CDCProperties): DataFrame

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
                         returnMature: Boolean = true): DataFrame

  /**
   * Filter a DataFrame based on the value of the 'active' column
   * @param allRecords the DataFrame to filter
   * @param properties the properties object
   * @param returnActive set to false to return records where active=false
   * @return a DataFrame
   */
  def filterOnActive(allRecords: DataFrame,
                     properties: CDCProperties,
                     returnActive: Boolean = true): DataFrame
}
