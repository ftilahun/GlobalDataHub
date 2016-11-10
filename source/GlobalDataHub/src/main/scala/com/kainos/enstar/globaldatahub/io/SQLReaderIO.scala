package com.kainos.enstar.globaldatahub.io

import org.apache.spark.{ Logging, SparkContext }
import org.apache.hadoop.fs.Path

/**
 * Created by ciaranke on 09/11/2016.
 */
class SQLReaderIO extends Logging {

  /**
   * Read a text file from the specified location and return the contents as a string
   * @param sparkContext the spark context
   * @param path the path to read from.
   * @return
   */
  def getStringFromFile( sparkContext : SparkContext, path : String ) = {
    logInfo( "reading from path: " + path )
    sparkContext.textFile( new Path( path ).toString ).collect().mkString( " " )
  }
}
