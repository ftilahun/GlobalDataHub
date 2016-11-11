package com.kainos.enstar.globaldatahub.io

import org.apache.spark.SparkContext

/**
 * Defines expected behaviour for reading a text file
 */
trait TextFileReder {

  def getStringFromFile( sparkContext : SparkContext, path : String ) : String

}
