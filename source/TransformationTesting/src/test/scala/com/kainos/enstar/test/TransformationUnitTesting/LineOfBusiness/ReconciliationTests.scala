package com.kainos.enstar.test.TransformationUnitTesting.LineOfBusiness

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{LineOfBusinessUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
 * Created by sionam on 28/11/2016.
 */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {

  test( "Reconciliation over test data" ) {

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

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/LineOfBusiness.hql" )
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/LineOfBusiness/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/LineOfBusiness/OutputRecordCount.hql" )

    // Act //
    lookup_block.registerTempTable( "lookup_block" )
    underwriting_block.registerTempTable( "underwriting_block" )
    val output = SQLRunner.runStatement( statement, sqlc )
    output.registerTempTable( "lineofbusiness" )

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )

    // Assert //
    assertDataFrameEquals( reconInput, reconOutput )
  }
}
