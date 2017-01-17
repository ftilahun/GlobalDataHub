package com.kainos.enstar.test.TransformationUnitTesting.Underwriter

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "Underwriter transformation mapping test" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val underwriter = utils.populateDataFrameFromCsvWithHeader( "/underwriter/input/underwriter_ndex_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedUnderwriter = utils.populateDataFrameFromCsvWithHeader( "/underwriter/output/underwriter_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Underwriter.hql" )

    // Act //
    underwriter.registerTempTable( "underwriter" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedUnderwriter, result )
  }

}
