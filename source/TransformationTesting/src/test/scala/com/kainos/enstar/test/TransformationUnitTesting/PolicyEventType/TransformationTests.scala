package com.kainos.enstar.test.TransformationUnitTesting.PolicyEventType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "PolicyEventTypeTransformation tes with Primary data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_renewal_status = utils.populateDataFrameFromCsvWithHeader( "/policyeventtype/input/lookup_renewal_status_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyEventType = utils.populateDataFrameFromCsvWithHeader( "/policyeventtype/output/policyeventtype_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/PolicyEventType.hql" )

    // Act //
    lookup_renewal_status.registerTempTable( "lookup_renewal_status" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyEventType, result )
  }
}
