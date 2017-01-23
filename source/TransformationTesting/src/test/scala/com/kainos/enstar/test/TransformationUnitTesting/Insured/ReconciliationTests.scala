package com.kainos.enstar.test.TransformationUnitTesting.Insured

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data for Insured" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val organisation = utils.populateDataFrameFromCsvWithHeader( "/ndex/insured/input/organisation/PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Insured.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Insured/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Insured/OutputRecordCount.hql" )

    // Act //
    organisation.registerTempTable( "organisation" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "ndex/insured" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
