package com.kainos.enstar.globaldatahub.cdcloader.io

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.exceptions.SQLException
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }
import com.kainos.enstar.globaldatahub.io.SQLReaderIO
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.mapred.InvalidInputException

/**
  * Unit tests for CDCSQLReaderIO
  */
class CDCSQLReaderIOSpec extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCSQLReaderIO" should "Retrieve a SQL query from HDFS" in {
    val sqlReaderIO = Mockito.mock( classOf[SQLReaderIO] )
    val cdcSQLReaderIO = new CDCSQLReaderIO( sqlReaderIO )
    val validQuery = "SELECT Column1 from table"
    val invalidQuery = "Not a SQL statement"
    Given( "The input /some/path/file.sql" )
    When( "The path is valid" )
    Mockito.when( sqlReaderIO.getStringFromFile( TestContexts.sparkContext, "/some/path/file.sql" ) )
      .thenReturn( validQuery )
    Then( "A SQL query should be returned" )
    cdcSQLReaderIO.getSQLString( TestContexts.sparkContext, "/some/path/file.sql" ) should be ( validQuery )

    Given( "The input /some/invalid/path" )
    When( "The path is invalid" )
    Mockito.when( sqlReaderIO.getStringFromFile( TestContexts.sparkContext, "/some/invalid/path" ) ).thenThrow( classOf[InvalidInputException] )
    Then( "An exception should be raised" )
    an[PathNotFoundException] should be thrownBy {
      cdcSQLReaderIO.getSQLString( TestContexts.sparkContext, "/some/invalid/path" )
    }

    Given( "The input /some/path/invalidfile.sql" )
    When( "The path is valid, but the file does not contain a SQL query" )
    Mockito.when( sqlReaderIO.getStringFromFile( TestContexts.sparkContext, "/some/path/invalidfile.sql" ) )
      .thenReturn( invalidQuery )
    Then( "An exception should be raised" )
    an[SQLException] should be thrownBy {
      cdcSQLReaderIO.getSQLString( TestContexts.sparkContext, "/some/path/invalidfile.sql" )
    }
  }

}
