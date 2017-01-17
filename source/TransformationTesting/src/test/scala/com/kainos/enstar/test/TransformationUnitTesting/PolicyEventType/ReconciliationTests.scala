package com.kainos.enstar.test.TransformationUnitTesting.PolicyEventType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data for PolicyEventType" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_renewal_status = utils.populateDataFrameFromCsvWithHeader( "/policyeventtype/input/lookup_renewal_status_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/PolicyEventType.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyEventType/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/PolicyEventType/OutputRecordCount.hql" )

    // Act //
    lookup_renewal_status.registerTempTable( "lookup_renewal_status" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "policyeventtype" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
