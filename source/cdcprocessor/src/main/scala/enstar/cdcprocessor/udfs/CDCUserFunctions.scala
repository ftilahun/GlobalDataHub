package enstar.cdcprocessor.udfs

import java.math.BigInteger

import enstar.cdcprocessor.properties.CDCProperties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by ciaranke on 16/12/2016.
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
   * @param tableName the name of the table being processed
   * @return a dataframe filtered by group
   */
  def groupByTransactionAndKey(df: DataFrame,
                               properties: CDCProperties,
                               tableName: String): DataFrame = {
    val changeNumber = udf(
      (s: String) => {
        //the length of the date string in the attunity change sequence
        val changeSeqDateLength = 16
        s.substring(changeSeqDateLength).toLong
      }
    )
    val grouped = df
      .withColumn("changeNumber",
        changeNumber(df(properties.changeSequenceColumnName)))
      .groupBy(
        df(properties.transactionColumnName),
        df(properties.idColumnName)
      )
      .max("changeNumber")
    df.join(grouped,
      changeNumber(df(properties.changeSequenceColumnName)) === grouped(
        "max(changeNumber)") &&
        df(properties.transactionColumnName) === grouped(
          properties.transactionColumnName),
      "inner")

  }

}
