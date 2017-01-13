package enstar.mailclient

/**
 * Thrown by MessageHlper when authentication is required and no
 * connection details are supplied.
 */
class ConnectionDetailsNotSetException(message: String = null,
                                       exception: Throwable = null)
    extends RuntimeException(message, exception) {}
