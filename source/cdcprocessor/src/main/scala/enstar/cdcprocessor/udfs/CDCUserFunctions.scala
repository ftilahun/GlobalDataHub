package enstar.cdcprocessor.udfs

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ DataFrame, SQLContext }
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
      "9999-12-31 23:59:59.000"
    }
  }

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
}
