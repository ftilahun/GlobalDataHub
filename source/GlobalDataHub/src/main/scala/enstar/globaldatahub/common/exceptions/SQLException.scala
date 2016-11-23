package enstar.globaldatahub.common.exceptions

import org.apache.spark.Logging

/**
 * Should be thrown when errors are found in SQL statements
 *
 * @param message the error message
 * @param sqlStatement the SQL statement that causeed the error.
 */
class SQLException( message : String, sqlStatement : String )
    extends RuntimeException
    with Logging {
  log.error( message, sqlStatement )
}
