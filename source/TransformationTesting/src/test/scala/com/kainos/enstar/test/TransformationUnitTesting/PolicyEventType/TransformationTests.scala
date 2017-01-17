package com.kainos.enstar.test.TransformationUnitTesting.PolicyEventType

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/policyeventtype/input/"
  private val testDataOutputDirPath = "/policyeventtype/output/"
  private val policyEventTypeTransformationPath = "Transformation/ndex/PolicyEventType.hql"

  test( "PolicyEventTypeTransformation test with Primary data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val lookup_renewal_status = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_renewal_status_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedPolicyEventType = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "policyeventtype_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( policyEventTypeTransformationPath )

    // Act //
    lookup_renewal_status.registerTempTable( "lookup_renewal_status" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicyEventType, result )
  }
}
