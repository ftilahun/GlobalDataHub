package enstar.cdcprocessor.exceptions

/**
  * Thrown when a required property is not found.
  *
  * @param message the message to display
  * @param exception the original exception
  */
class PropertyNotSetException(message: String = null, exception: Throwable = null)
  extends RuntimeException(message, exception)
