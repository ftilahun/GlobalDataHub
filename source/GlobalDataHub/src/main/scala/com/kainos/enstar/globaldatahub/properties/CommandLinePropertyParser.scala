package com.kainos.enstar.globaldatahub.properties

/**
 * Expected behaviour for a command line parser.
 */
trait CommandLinePropertyParser {

  /**
   * Map an array of strings in k1=v1,k2=v2 format to a Map[String,String]
   *
   * @param propertyArray the string array to map
   * @return a Map of values
   */
  def parseProperties( propertyArray : Array[String] ) : Map[String, String]
}
