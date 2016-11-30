package com.kainos.enstar.test.TransformationUnitTesting.RiskCode

/**
 * Created by adamf on 30/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ RiskCodeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "RiskCode Mapping reconciliation over test data" ) {

    // Arrange //
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_risk_code : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/riskcode/input/lookup_riskcode_test1.csv" ).toString,
      getClass.getResource( "/riskcode/schemas/lookup_riskcode.avro" ).toString,
      _.split( "," ),
      RiskCodeUtils.lookupRiskCodeMapping,
      sqlContext
    )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/RiskCode.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/RiskCode/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/RiskCode/OutputRecordCount.hql" )

    // Act //
    lookup_risk_code.registerTempTable( "lookup_risk_code" )

    val output = SQLRunner.runStatement( hqlStatement, sqlc )
    output.registerTempTable( "riskcode" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }

}
