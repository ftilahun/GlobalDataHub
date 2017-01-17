package com.kainos.enstar.test.TransformationUnitTesting.Geography

/**
 * Created by adamf on 29/11/2016.
 */
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils
  private val testDataInputDirPath = "/geography/input/"
  private val testDataOutputDirPath = "/geography/output/"
  private val geographyTransformationPath = "Transformation/ndex/Geography.hql"

  test( "Geography Transformation with Primary data" ){

    // Arrange
    implicit val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val lookupCountry = utils.populateDataFrameFromCsvWithHeader( testDataInputDirPath + "lookup_country_PrimaryTestData.csv" )

    val expectedGeographyMapping = utils.populateDataFrameFromCsvWithHeader( testDataOutputDirPath + "geography_PrimaryTestData.csv" )

    lookupCountry.registerTempTable( "lookup_country" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( geographyTransformationPath )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedGeographyMapping, result )
  }
}