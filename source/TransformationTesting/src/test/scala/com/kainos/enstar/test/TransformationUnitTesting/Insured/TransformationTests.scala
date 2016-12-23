package com.kainos.enstar.test.TransformationUnitTesting.Insured

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "InsuredTransformation tes with Primary data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val organisation = utils.populateDataFrameFromCsvWithHeader( "/insured/input/organisation_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedInsured = utils.populateDataFrameFromCsvWithHeader( "/insured/output/insured_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Insured.hql" )

    // Act //
    organisation.registerTempTable( "organisation" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedInsured, result )
  }
}
