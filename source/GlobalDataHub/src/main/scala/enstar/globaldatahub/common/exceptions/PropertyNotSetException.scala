package enstar.globaldatahub.common.exceptions

import org.apache.spark.Logging

/**
 * Thrown when a required property is not found.
 *
 * @param message the message to display
 * @param exception the original exception
 */
class PropertyNotSetException( message : String = null, exception : Throwable = null )
  extends RuntimeException( message, exception )
