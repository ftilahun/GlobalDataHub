package enstar.cdctableprocessor.exceptions

/**
 * Thrown when unable to read a DataFrame from disk.
 */
class DataFrameReadException(message: String = null,
                             exception: Throwable = null)
    extends RuntimeException(message, exception)
