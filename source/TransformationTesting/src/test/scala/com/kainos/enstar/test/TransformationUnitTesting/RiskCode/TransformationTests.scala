package com.kainos.enstar.test.TransformationUnitTesting.RiskCode

/**
 * Created by adamf on 30/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ RiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "RiskCodeTransformation_test1" ){

    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/riskcode/input/lookup_riskcode_test1.csv" ).toString,
      getClass.getResource( "/riskcode/schemas/lookup_riskcode.avro" ).toString,
      _.split( "," ),
      RiskCodeUtils.lookupRiskCodeMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedRiskCode : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/riskcode/output/riskcode_test1.csv" ).toString,
      getClass.getResource( "/riskcode/schemas/riskcode.avro" ).toString,
      _.split( "," ),
      RiskCodeUtils.riskCodeMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/RiskCode.hql" )

    // Act //
    lookup_risk_code.registerTempTable( "lookup_risk_code" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedRiskCode, result )
  }
}

