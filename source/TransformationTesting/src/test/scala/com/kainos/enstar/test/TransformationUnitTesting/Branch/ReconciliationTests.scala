package com.kainos.enstar.test.TransformationUnitTesting.Branch

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data for Branch" ) {

    // Arrange //
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( "/branch/input/lookup_profit_centre_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Branch.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Branch/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Branch/OutputRecordCount.hql" )

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "branch" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
