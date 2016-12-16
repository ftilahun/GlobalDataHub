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
  def dropAttunityColumns(df: DataFrame,
                          properties: CDCProperties): DataFrame
}
