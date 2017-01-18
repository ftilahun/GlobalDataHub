package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.io.DataFrameWriter
import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.{ Column, DataFrame, SQLContext }
import org.apache.spark.storage.StorageLevel

trait UserFunctions extends Serializable {

  /**
   * Join two DataFrames
   *
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
   * @param isDeleted flag indicating if the record has been deleted
   * @param validTo the valid from date of the subsequent row
   * @param transactionTimeStamp the transaction timestamp
   * @param properties the properties object
   * @return the valid to date for this row (as a string)
   */
  def updateClosedRecord(isDeleted: Boolean,
                         validTo: String,
                         transactionTimeStamp: String,
                         properties: CDCProperties): String

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
                   timestampColumnName: String): DataFrame

  /**
   * Add an 'active' column to the passed in DataFrame based on the value of the transaction timestamp
   * @param dataFrame the DataFrame to add the column to
   * @param properties the properties object
   * @return a DataFrame
   */
  def addActiveColumn(dataFrame: DataFrame,
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
                          doNotSetFlag: Boolean = false): DataFrame

  /**
   * Drops a column from the DataFrame
   * @param dataFrame the DataFrame to operate on
   * @param columnName the name of the column to drop
   * @return A DataFrame
   */
  def dropColumn(dataFrame: DataFrame, columnName: String): DataFrame

  /**
   * Add a column to a DataFrame
   * @param dataFrame the DataFrame to add to
   * @param columnName the name of the new column
   * @param column the column to add
   * @return a DataFrame
   */
  def addColumn(dataFrame: DataFrame,
                columnName: String,
                column: Column): DataFrame

  /**
   * Return a specific column from a DataFrame
   * @param dataFrame the DataFrame to extract from
   * @param columnName the name of the column to extract
   * @return a Column
   */
  def getColumn(dataFrame: DataFrame, columnName: String): Column

  /**
   * Persist a DataFrame at the specified storage level
   * @param dataFrame the DataFrame to persist
   * @param storageLevel the StorageLevel to persist at
   * @param properties the properties object
   * @return
   */
  def persistForMetrics(dataFrame: DataFrame,
                        storageLevel: StorageLevel,
                        properties: CDCProperties): Unit

  /**
   * Get a count of the rows in a DataFrame
   * @param dataFrame the DataFrame to count
   * @return the number of rows
   */
  def getCount(dataFrame: DataFrame): Long

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
                   storageLevel: StorageLevel): Unit
}
