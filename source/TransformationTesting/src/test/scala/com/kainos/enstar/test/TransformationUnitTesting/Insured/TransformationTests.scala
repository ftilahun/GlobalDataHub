package com.kainos.enstar.test.TransformationUnitTesting.Insured

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/insured/input/"
  private val testDataOutputDirPath = "/insured/output/"
  private val insuredTransformationPath = "Transformation/Insured.hql"

  test( "InsuredTransformation test with Primary data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val organisation = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "organisation_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedInsured = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "insured_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( insuredTransformationPath )

    // Act //
    organisation.registerTempTable( "organisation" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedInsured, result )
  }
}
