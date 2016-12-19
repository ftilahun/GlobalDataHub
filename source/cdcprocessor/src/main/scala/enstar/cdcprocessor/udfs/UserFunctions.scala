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
  def closeRecords(dataFrame: DataFrame,
                   properties: CDCProperties): DataFrame
}
