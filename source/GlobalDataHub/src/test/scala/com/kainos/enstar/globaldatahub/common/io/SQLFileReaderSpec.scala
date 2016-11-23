package com.kainos.enstar.globaldatahub.common.io

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.common.exceptions.SQLException
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.SparkContext
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCSQLReader
 */
class SQLFileReaderSpec extends FlatSpec with GivenWhenThen with Matchers {

  "SQLFileReader" should "Retrieve a SQL query from HDFS" in {
    val textReader = Mockito.mock( classOf[TextFileReader] )
    val cdcSQLReader = new SQLFileReader( textReader )
    val validQuery = "SELECT Column1 from table"
    val invalidQuery = "Not a SQL statement"
    Given( "The input /some/path/file.sql" )
    When( "The path is valid" )
    Mockito
      .when(
        textReader.getStringFromFile( TestContexts.sparkContext,
          "/some/path/file.sql" ) )
      .thenReturn( validQuery )
    Then( "A SQL query should be returned" )
    cdcSQLReader
      .getSQLString( TestContexts.sparkContext, "/some/path/file.sql" ) should be(
        validQuery )

    Given( "The input /some/invalid/path" )
    When( "The path is invalid" )
    Mockito
      .when(
        textReader.getStringFromFile( TestContexts.sparkContext,
          "/some/invalid/path" ) )
      .thenThrow( classOf[InvalidInputException] )
    Then( "An exception should be raised" )
    an[PathNotFoundException] should be thrownBy {
      cdcSQLReader.getSQLString( TestContexts.sparkContext,
        "/some/invalid/path" )
    }

    Given( "The input /some/path/invalidfile.sql" )
    When( "The path is valid, but the file does not contain a SQL query" )
    Mockito
      .when(
        textReader.getStringFromFile( TestContexts.sparkContext,
          "/some/path/invalidfile.sql" ) )
      .thenReturn( invalidQuery )
    Then( "An exception should be raised" )
    an[SQLException] should be thrownBy {
      cdcSQLReader.getSQLString( TestContexts.sparkContext,
        "/some/path/invalidfile.sql" )
    }

    Mockito
      .verify( textReader, Mockito.times( 3 ) )
      .getStringFromFile( org.mockito.Matchers.any( classOf[SparkContext] ),
        org.mockito.Matchers.anyString() )
  }

}
