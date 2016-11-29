package enstar.globaldatahub.common.io

import enstar.globaldatahub.common.exceptions.SQLException
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.{ Logging, SparkContext }

/**
 * Class to read a SQL statement from the filesystem
 *
 * @param textFileReader a TextFileReader
 */
class SQLFileReader(textFileReader: FileReader)
    extends SQLReader
    with Logging {

  /**
   * Returns a SQL statement from the passed in file path.
   *
   * @param sparkContext the spark context
   * @param path the path to read from
   * @return a sql statement
   */
  def getSQLString(sparkContext: SparkContext, path: String): String = {
    try {
      logInfo("reading from: " + path)
      val sql = textFileReader.getStringFromFile(sparkContext, path)
      if (!sql.matches("^(?i)SELECT.+from.+")) {
        //this is not a SQL Statement
        logError("Could not find SQL statment in " + path)
        throw new SQLException("Not an SQL statement", sql)
      } else {
        logInfo("Got sql statement: " + sql)
        sql
      }
    } catch {
      case e: InvalidInputException => throw new PathNotFoundException(path)
      case e: SQLException          => throw e
    }
  }
}
