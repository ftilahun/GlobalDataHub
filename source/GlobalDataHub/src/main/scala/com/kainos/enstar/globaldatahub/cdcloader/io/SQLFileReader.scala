package com.kainos.enstar.globaldatahub.cdcloader.io

import org.apache.spark.SparkContext

/**
 * Defines expected behaviour for a SQLFileReader
 */
trait SQLFileReader {

  /**
   * Returns a SQL statement from the passed in file path.
   *
   * @param sparkContext the spark context
   * @param path the path to read from
   * @return a sql statement
   */
  def getSQLString( sparkContext : SparkContext, path : String ) : String
}
