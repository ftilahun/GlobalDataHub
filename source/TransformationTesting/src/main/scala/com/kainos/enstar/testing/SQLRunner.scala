package com.kainos.enstar.testing

import java.io.InputStream

import org.apache.spark.sql.{ DataFrame, SQLContext }

/**
 * Created by terences on 20/11/2016.
 */
object SQLRunner {

  def loadStatementFromResource( filename : String )() : String = {
    val stream : InputStream = getClass.getResourceAsStream( "/" + filename )
    val lines = scala.io.Source.fromInputStream( stream ).mkString
    stream.close()
    lines
  }

  def runStatement( statement : String, sqlContext : SQLContext ) : DataFrame = {
    sqlContext.sql( statement )
  }

}
