package com.kainos.enstar.globaldatahub.common.io

import org.apache.spark.SparkContext

/**
 * Defines expected behaviour for reading a text file
 */
trait FileReader {

  /**
   * Read a text file from the specified location and return the contents as a string
   * @param sparkContext the spark context
   * @param path the path to read from.
   * @return
   */
  def getStringFromFile( sparkContext : SparkContext, path : String ) : String

}
