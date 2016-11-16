package com.kainos.enstar.globaldatahub.exceptions

import org.apache.spark.Logging

/**
 * Thrown when a required property is not found.
 *
 * @param message the message to display
 * @param exception the original exception
 */
class PropertyNotSetException( message : String, exception : Exception )
    extends RuntimeException
    with Logging {
  log.error( message )
  log.error( "Caused by: " + exception.getClass )
  log.error( exception.getStackTraceString )
}