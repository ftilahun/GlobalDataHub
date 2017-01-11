package enstar.cdcprocessor.properties

/**
 * Defines expected behaviour for creating the properties object
 */
trait PropertyParser[T] {

  /**
   * parse any arguments passed to the applicatin to create
   * the Properies object
   * @param args arguments to program
   * @return the properties object
   */
  def parse(args: T): CDCProperties
}
