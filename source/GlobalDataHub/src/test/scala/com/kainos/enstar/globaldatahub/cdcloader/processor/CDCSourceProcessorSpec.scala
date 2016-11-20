package com.kainos.enstar.globaldatahub.cdcloader.processor

import com.kainos.enstar.globaldatahub.TestContexts
import com.kainos.enstar.globaldatahub.cdcloader.control.ControlProcessor
import com.kainos.enstar.globaldatahub.cdcloader.io.{
  DataFrameReader,
  DataFrameWriter,
  SQLFileReader,
  TableOperations
}
import com.kainos.enstar.globaldatahub.cdcloader.udfs.UserFunctions
import com.kainos.enstar.globaldatahub.common.properties.GDHProperties
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.mockito.Mockito
import org.scalatest.{ FlatSpec, GivenWhenThen, Matchers }

/**
 * Unit tests for CDCSourceProcessor
 */
class CDCSourceProcessorSpec
    extends FlatSpec
    with GivenWhenThen
    with Matchers {

  "CDCSourceProcessor" should "Process each table exactly once" in {

    val cdcSourceProcessor = new CDCSourceProcessor
    val controlProcessor = Mockito.mock( classOf[ControlProcessor] )
    val properties = Mockito.mock( classOf[GDHProperties] )
    val reader = Mockito.mock( classOf[DataFrameReader] )
    val writer = Mockito.mock( classOf[DataFrameWriter] )
    val tableOperations = Mockito.mock( classOf[TableOperations] )
    val tableProcessor = Mockito.mock( classOf[TableProcessor] )
    val userFunctions = Mockito.mock( classOf[UserFunctions] )
    val sqlReader = Mockito.mock( classOf[SQLFileReader] )

    Given( "Three tables" )
    val tables = Array[String](
      "policy",
      "customer",
      "claim"
    )
    Mockito
      .when( properties.getArrayProperty( "spark.cdcloader.input.tablenames" ) )
      .thenReturn( tables )

    When( "Processing" )
    cdcSourceProcessor.process(
      controlProcessor,
      properties,
      TestContexts.sqlContext,
      reader,
      writer,
      tableOperations,
      tableProcessor,
      userFunctions,
      sqlReader
    )

    Then( "Each table should process once" )
    for ( i <- 0 to 2 ) {
      Mockito
        .verify( tableProcessor, Mockito.times( 1 ) )
        .process( tables( i ),
          TestContexts.sqlContext,
          controlProcessor,
          properties,
          reader,
          userFunctions,
          tableOperations,
          sqlReader )
    }

    Then( "Each table should be saved once" )
    Mockito
      .verify( tableProcessor, Mockito.times( 3 ) )
      .save(
        org.mockito.Matchers.any( classOf[SQLContext] ),
        org.mockito.Matchers.any( classOf[DataFrameWriter] ),
        org.mockito.Matchers.any( classOf[GDHProperties] ),
        org.mockito.Matchers.any( classOf[DataFrame] ),
        org.mockito.Matchers.anyString()
      )
  }
}
