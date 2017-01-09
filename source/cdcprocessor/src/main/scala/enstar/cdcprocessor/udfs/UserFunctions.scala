package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.DataFrame

trait UserFunctions extends Serializable {

  /**
    * * Provides a string representation of the current time in the specified
    * format
    * @param format the string format
    * @return a string representation of the current timestamp
    */
  def getCurrentTime(format: String): String

  /**
    * Group changes in a dataframe by transaction
    * @param df the dataframe to group
    * @param properties the GDH properties object
    * @return a dataframe filtered by group
    */
  def groupByTransactionAndKey(df: DataFrame,
                               properties: CDCProperties): DataFrame

  /**
    * Drops attunity columns from the output dataset
    * @param df the dataframe to operate on
    * @param properties the properties object
    * @return a dataframe
    */
  def dropAttunityColumns(df: DataFrame, properties: CDCProperties): DataFrame

  /**
    * Filter before image records from the dataframe
    * @param dataFrame the dataframe to filter
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
    * Close records that have been superseded in the dataframe
    * @param dataFrame the dataframe to process
    * @param properties the properties object
    * @return a dataframe
    */
  def closeRecords(dataFrame: DataFrame, properties: CDCProperties): DataFrame

  /**
    * Add an 'active' column to the passed in dataframe based on the value of the transaction timestamp
    * @param dataFrame the dataframe to add the column to
    * @param properties the properties object
    * @return a dataframe
    */
  def addActiveColumn(dataFrame: DataFrame,
                      properties: CDCProperties): DataFrame

  /**
    * drop the active column from the passed in dataframe
    * @param dataFrame the dataframe to operate on
    * @param properties the properties object
    * @return a dataframe
    */
  def dropActiveColumn(dataFrame: DataFrame,
                       properties: CDCProperties): DataFrame

  /**
    * Filter a dataframe based on a time window, this method filters the passed in DF based on the
    * value in the transaction timestamp column.
    * @param dataFrame the dataframe to filter
    * @param properties the properties object
    * @param returnMature set to true to only return data that is older than the time window
    * @return a dataframe
    */
  def filterOnTimeWindow(dataFrame: DataFrame,
                         properties: CDCProperties,
                         returnMature: Boolean = true): DataFrame
}
