package enstar.globaldatahub.common.io

import org.apache.spark.{ Logging, SparkContext }
import org.apache.hadoop.fs.Path

/**
 * Helper class for reading SQL statements from HDFS
 */
class TextFileReader extends Logging with FileReader {

  /**
   * Read a text file from the specified location and return the contents as a string
   * @param sparkContext the spark context
   * @param path the path to read from.
   * @return
   */
  def getStringFromFile( sparkContext : SparkContext, path : String ) : String = {
    logInfo( "reading from path: " + path )
    sparkContext.textFile( new Path( path ).toString ).collect().mkString( " " )
  }
}
