package com.kainos.enstar.globaldatahub.common.exceptions

import org.apache.spark.Logging

/**
 * Thrown when a required property is not found.
 *
 * @param message the message to display
 * @param exception the original exception
 */
class PropertyNotSetException( message : String, exception : Option[Exception] )
    extends RuntimeException
    with Logging {
  log.error( message )
  if ( exception.isDefined ) {
    log.error( "Caused by: " + exception.get.getClass )
    log.error( exception.get.getStackTraceString )
  }
}
