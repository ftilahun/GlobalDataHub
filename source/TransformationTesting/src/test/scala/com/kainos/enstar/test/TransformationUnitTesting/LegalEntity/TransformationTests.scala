package com.kainos.enstar.test.TransformationUnitTesting.LegalEntity

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

/**
 * Created by terences on 21/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/legalentity/input/"
  private val testDataOutputDirPath = "/legalentity/output/"
  private val legalEntityTransformationPath = "Transformation/LegalEntity.hql"

  test( "LegalEntity transformation mapping test with primary test data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val lookup_profit_centre = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_profit_centre_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedLegalEntity = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "legalentity_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( legalEntityTransformationPath )

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedLegalEntity, result.orderBy( "legalentitycode" ) )
  }
}
