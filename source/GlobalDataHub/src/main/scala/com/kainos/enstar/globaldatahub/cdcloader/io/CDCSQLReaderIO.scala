package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.exceptions.SQLException
import com.kainos.enstar.globaldatahub.io.SQLReaderIO
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext

class CDCSQLReaderIO( sqlReaderIO : SQLReaderIO ) {

  def getSQLString( sparkContext : SparkContext, path : String ) : String = {
    try {
      val sql = sqlReaderIO.getStringFromFile( sparkContext, path )
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
