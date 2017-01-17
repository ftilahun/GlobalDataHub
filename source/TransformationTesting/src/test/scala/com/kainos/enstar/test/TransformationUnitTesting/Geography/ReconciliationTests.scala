package com.kainos.enstar.test.TransformationUnitTesting.Geography

/**
 * Created by adamf on 29/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Geography Mapping reconciliation over test data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframes
    val lookupCountry = utils.populateDataFrameFromCsvWithHeader( "/geography/input/lookup_country_PrimaryTestData.csv" )

    // Load the hql statement under test
    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/Geography.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Geography/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Geography/OutputRecordCount.hql" )

    // Act //
    lookupCountry.registerTempTable( "lookup_country" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "geography" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
