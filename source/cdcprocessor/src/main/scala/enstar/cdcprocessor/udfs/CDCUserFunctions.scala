package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * User functions
 */
class CDCUserFunctions extends UserFunctions {

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
    val changeNumber = udf(
      (s: String) => {
        //the length of the date string in the attunity change sequence
        val changeSeqDateLength = 16
        s.substring(changeSeqDateLength).toLong
      }
    )
    val grouped = df
      .withColumn(properties.attunityColumnPrefix + "changeNumber",
        changeNumber(df(properties.changeSequenceColumnName)))
      .groupBy(
        df(properties.transactionColumnName),
        df(properties.idColumnName)
      )
      .max(properties.attunityColumnPrefix + "changeNumber")
    df.join(grouped,
      changeNumber(df(properties.changeSequenceColumnName)) === grouped(
        s"max(${properties.attunityColumnPrefix}changeNumber)") &&
        df(properties.transactionColumnName) === grouped(
          properties.transactionColumnName),
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
    df.select(df.columns.filter(!_.contains(properties.attunityColumnPrefix)
    ).map(col): _*)

}
