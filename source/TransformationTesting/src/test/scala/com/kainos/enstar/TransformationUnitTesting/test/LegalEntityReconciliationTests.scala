package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ LegalEntityUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 24/11/2016.
 */
class LegalEntityReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data" ) {

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/legalentity/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/legalentity/schemas/lookup_profit_centre.avro" ).toString,
      _.split( "," ),
      LegalEntityUtils.lookupProfitCentreMapping,
      sqlContext
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "LegalEntity.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource("Reconciliation/LegalEntity/ReconInputTest1.hql")
    val reconStatementOutput = utils.loadHQLStatementFromResource("Reconciliation/LegalEntity/ReconOutputTest1.hql")

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "legalentity" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
