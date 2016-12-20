package com.kainos.enstar.transformation.udf

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting._
import org.scalatest.FunSuite

/**
 * Created by terences on 15/12/2016.
 */
class NetAsPctOfGrossTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  test( "NetAsPctOfGross aggregation with monotonic increasing sequence with multiple sequence number groups" ) {

    // Arrange
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val input = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/input/input_MonotonicSeqMultipleSeqGroups.csv" )
    val expectedOutput = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/output/output_MonotonicSeqMultipleSeqGroups.csv" )
    val statement = utils.loadHQLStatementFromResource( "UDAF/NetAsPctOfGross/query/NetAsPctOfGross.hql" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross );
    input.registerTempTable( "input" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedOutput.orderBy( "id" ), result.orderBy( "id" ) )

  }

  test( "NetAsPctOfGross aggregation with non monotonic increasing sequence with multiple sequence number groups" ) {

    // Arrange
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val input = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/input/input_NonMonotonicSeqMultipleSeqGroups.csv" )
    val expectedOutput = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/output/output_NonMonotonicSeqMultipleSeqGroups.csv" )
    val statement = utils.loadHQLStatementFromResource( "UDAF/NetAsPctOfGross/query/NetAsPctOfGross.hql" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross );
    input.registerTempTable( "input" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedOutput.orderBy( "id" ), result.orderBy( "id" ) )

  }

  test( "NetAsPctOfGross aggregation with non monotonic increasing sequence with multiple sequence number groups and unordered input" ) {

    // Arrange
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val input = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/input/input_NonMonotonicSeqMultipleSeqGroupsUnordered.csv" )
    val expectedOutput = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/output/output_NonMonotonicSeqMultipleSeqGroupsUnordered.csv" )
    val statement = utils.loadHQLStatementFromResource( "UDAF/NetAsPctOfGross/query/NetAsPctOfGross.hql" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross );
    input.registerTempTable( "input" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedOutput.orderBy( "id" ), result.orderBy( "id" ) )

  }

  test( "NetAsPctOfGross aggregation with multiple LayerIds" ) {

    // Arrange
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    val input = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/input/input_MultipleLayerIds.csv" )
    val expectedOutput = utils.populateDataFrameFromCsvWithHeader( "/UDAF/NetAsPctOfGross/output/output_MultipleLayerIds.csv" )
    val statement = utils.loadHQLStatementFromResource( "UDAF/NetAsPctOfGross/query/NetAsPctOfGross.hql" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross );
    input.registerTempTable( "input" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedOutput.orderBy( "id" ), result.orderBy( "id" ) )

  }
}
