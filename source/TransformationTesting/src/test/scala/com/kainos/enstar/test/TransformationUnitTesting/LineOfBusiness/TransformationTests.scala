package com.kainos.enstar.test.TransformationUnitTesting.LineOfBusiness

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{LineOfBusinessUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by sionam on 24/11/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "LineOfBusinessTransformationMappingTest" ){

    // Arrange //
    // Use sqlContext from spark-testing-base
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    // Load test data into dataframe
    val lookup_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/lineofbusiness/input/lookup_block_test1.csv" ).toString,
      getClass.getResource( "/lineofbusiness/schemas/lookup_block.avro" ).toString,
      _.split(","),
      LineOfBusinessUtils.lookupBlockMapping,
      sqlc
    )

    val underwriting_block : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/lineofbusiness/input/underwriting_block_test1.csv" ).toString,
      getClass.getResource( "/lineofbusiness/schemas/underwriting_block.avro" ).toString,
      _.split(","),
      LineOfBusinessUtils.underwritingBlockMapping,
      sqlc
    )

    // Load expected result into dataframe
    val expectedLineOfBusiness : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/lineofbusiness/output/line_of_business_test1.csv" ).toString,
      getClass.getResource( "/lineofbusiness/schemas/lineofbusiness.avro" ).toString,
      _.split(","),
      LineOfBusinessUtils.lineOfBusinessMapping,
      sqlc
    )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource("Transformation/LineOfBusiness.hql")

    // Act //
    lookup_block.registerTempTable( "lookup_block" )
    underwriting_block.registerTempTable( "underwriting_block" )
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert //
    assertDataFrameEquals( expectedLineOfBusiness.orderBy("lineofbusinesscode"), result.orderBy("lineofbusinesscode") )
  }
}
