package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{DeductionUtils, NetAsPctOfGross, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

/**
  * Created by terences on 19/12/2016.
  */
class ReconciliationTests extends FunSuite with DataFrameSuiteBase {


  private val utils = new TransformationUnitTestingUtils
  private val testDataInputPath = "/deduction/input/"
  private val testDataOutputPath = "/deduction/output/"
  private val schemasPath = "/deduction/schemas/"

  def populateDataFrameWithLineTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "line.avro" ).toString,
      _.split( "," ),
      DeductionUtils.lineMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "layer.avro" ).toString,
      _.split( "," ),
      DeductionUtils.layerMapping,
      sqlc
    )
  }

  def populateDataFrameWithLayerDeductionTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataInputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "layer_deduction.avro" ).toString,
      _.split( "," ),
      DeductionUtils.layerDeductionMapping,
      sqlc
    )
  }

  def populateDataFrameWithDeductionTestData( dataFile : String, sqlc : SQLContext ) : DataFrame = {
    utils.populateDataFrameFromFile(
      getClass.getResource( testDataOutputPath + dataFile ).toString,
      getClass.getResource( schemasPath + "deduction.avro" ).toString,
      _.split( "," ),
      DeductionUtils.deductionMapping,
      sqlc
    )
  }

  test( "Reconciliation over test data" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionNonMonotonicSeqOutOfOrder.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql")
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/OutputRecordCount.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )
    result.registerTempTable("deduction")

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )


    // Assert
    assertDataFrameEquals( reconInput, reconOutput )

  }

  test( "Reconciliation over test data with multiple lines" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_MultipleLines.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_MultipleLines.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

    // Load the hql statement under test
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql")
    val reconStatementInput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/InputRecordCount.hql" )
    val reconStatementOutput = utils.loadHQLStatementFromResource( "Reconciliation/Deduction/OutputRecordCount.hql" )

    // Act
    val result = SQLRunner.runStatement( statement, sqlc )
    result.registerTempTable("deduction")

    val reconInput = SQLRunner.runStatement( reconStatementInput, sqlc )
    val reconOutput = SQLRunner.runStatement( reconStatementOutput, sqlc )


    // Assert
    assertDataFrameEquals( reconInput, reconOutput )

  }
}
