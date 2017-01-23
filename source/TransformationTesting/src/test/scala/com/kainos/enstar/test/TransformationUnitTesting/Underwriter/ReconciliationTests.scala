package com.kainos.enstar.test.TransformationUnitTesting.Underwriter

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.Matchers
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase with Matchers {

  test( "ndex.underwriter Mapping reconciliation over test data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val ndex_underwriter = utils.populateDataFrameFromCsvWithHeader( "/ndex/underwriter/input/underwriter/PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Underwriter.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Underwriter/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Underwriter/OutputRecordCount.hql" )

    // Act //
    ndex_underwriter.registerTempTable( "underwriter" )
    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )

    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "underwriter" )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
