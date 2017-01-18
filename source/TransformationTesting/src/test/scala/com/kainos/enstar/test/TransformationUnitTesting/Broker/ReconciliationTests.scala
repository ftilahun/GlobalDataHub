package com.kainos.enstar.test.TransformationUnitTesting.Broker

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data for Broker" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val broking_company = utils.populateDataFrameFromCsvWithHeader( "/ndex/broker/input/broking_company/PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Broker.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Broker/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Broker/OutputRecordCount.hql" )

    // Act //
    broking_company.registerTempTable( "broking_company" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "broker" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
