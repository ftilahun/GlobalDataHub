package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.exceptions.SQLException
import com.kainos.enstar.globaldatahub.io.FileReader
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext

/**
 * Class to read a SQL statement from the filesystem
 *
 * @param textFileReader a TextFileReader
 */
class CDCSQLFileReader( textFileReader : FileReader ) extends SQLFileReader {

  /**
   * Returns a SQL statement from the passed in file path.
   *
   * @param sparkContext the spark context
   * @param path the path to read from
   * @return a sql statement
   */
  def getSQLString( sparkContext : SparkContext, path : String ) : String = {
    try {
      val sql = textFileReader.getStringFromFile( sparkContext, path )
      if ( !sql.matches( "^(?i)SELECT.+from.+" ) ) {
        //this is not a SQL Statement
        throw new SQLException( "Not an SQL statement", sql )
      } else {
        sql
      }
    } catch {
      case e : InvalidInputException => throw new PathNotFoundException( path )
      case e : SQLException          => throw e
    }
  }
}
