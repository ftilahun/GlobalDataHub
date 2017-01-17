package com.kainos.enstar.test.TransformationUnitTesting.Broker

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ SQLRunner, TransformationUnitTestingUtils }
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "BrokerTransformation test with Primary data" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    implicit val sqlc = sqlContext
    sqlc.sparkContext.setLogLevel( "WARN" )
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val brokingCompany = utils.populateDataFrameFromCsvWithHeader( "/broker/input/broking_company_PrimaryTestData.csv" )

    // Load expected result into dataframe
    val expectedBroker = utils.populateDataFrameFromCsvWithHeader( "/broker/output/broker_PrimaryTestData.csv" )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/ndex/Broker.hql" )

    // Act //
    brokingCompany.registerTempTable( "broking_company" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedBroker, result )
  }
}
