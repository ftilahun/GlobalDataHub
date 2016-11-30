package enstar.globaldatahub.common.exceptions

/**
 * Thrown when unable to persist a DataFrame to disk.
 */
class DataFrameWriteException(message: String = null, exception: Throwable = null)
  extends RuntimeException(message, exception)
