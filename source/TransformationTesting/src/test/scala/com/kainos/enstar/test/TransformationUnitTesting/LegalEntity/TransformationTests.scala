package com.kainos.enstar.test.TransformationUnitTesting.LegalEntity

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ LegalEntityUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by terences on 21/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "LegalEntityTransformation_test1" ){

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

    // Load expected result into dataframe
    val expectedLegalEntity : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/legalentity/output/legalentity_test1.csv" ).toString,
      getClass.getResource( "/legalentity/schemas/legalentity.avro" ).toString,
      _.split( "," ),
      LegalEntityUtils.legalEntityMapping,
      sqlContext
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/LegalEntity.hql" )

    // Act //
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedLegalEntity, result )
  }
}
