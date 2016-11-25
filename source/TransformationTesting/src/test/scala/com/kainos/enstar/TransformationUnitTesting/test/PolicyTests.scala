package com.kainos.enstar.TransformationUnitTesting.test

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 23/11/2016.
 */
class PolicyTests extends FunSuite with DataFrameSuiteBase {

  test( "PolicyTransformation_test1" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils
    sqlc.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframe
    val layer : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/layer_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/layer.avro" ).toString,
      PolicyUtils.layerMapping,
      sqlc
    )

    val line : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/line_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/line.avro" ).toString,
      PolicyUtils.lineMapping,
      sqlc
    )

    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_block.avro" ).toString,
      PolicyUtils.lookupBlockMapping,
      sqlc
    )

    val lookup_profit_centre : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/lookup_profit_centre_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/lookup_profit_centre.avro" ).toString,
      PolicyUtils.lookupProfitCentreMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/underwriting_block.avro" ).toString,
      PolicyUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedPolicy : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/policy/output/policy_test1.csv" ).toString,
      getClass.getResource( "/policy/schemas/policy.avro" ).toString,
      PolicyUtils.policyMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Policy.hql" )

    // Act //
    layer.registerTempTable( "layer" )
    line.registerTempTable( "line" )
    lookup_block.registerTempTable( "lookup_block" )
    lookup_profit_centre.registerTempTable( "lookup_profit_centre" )
    underwriting_block.registerTempTable( "underwriting_block" )

    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedPolicy, result )
  }

}
