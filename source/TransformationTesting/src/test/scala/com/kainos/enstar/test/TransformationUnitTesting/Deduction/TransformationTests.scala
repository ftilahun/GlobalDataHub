package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{DeductionUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql._
import org.scalatest.FunSuite
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * Created by terences on 08/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

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

  test( "Deduction transformation mapping test with primary data sets" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_PrimaryTestData.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_PrimaryTestData.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    // Load the hql statement under test
    val tmptableStatement = utils.loadHQLStatementFromResource("Transformation/DeductionPre.hql")
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val tmptable = SQLRunner.runStatement(tmptableStatement, sqlc).registerTempTable("tmptable")
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )
  }

  test( "Deduction transformation mapping test calculating CalculatedDeductionAmount for policy with single deduction" ){

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_SingleDeductionCalculatedDeductionAmount.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_SingleDeductionCalculatedDeductionAmount.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_SingleDeductionCalculatedDeductionAmount.csv", sqlc )

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_SingleDeductionCalculatedDeductionAmount.csv", sqlc )

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    // Load the hql statement under test
    val tmptableStatement = utils.loadHQLStatementFromResource("Transformation/DeductionPre.hql")
    val statement = utils.loadHQLStatementFromResource( "Transformation/Deduction.hql" )

    // Act
    val tmptable = SQLRunner.runStatement(tmptableStatement, sqlc).registerTempTable("tmptable")
    val result = SQLRunner.runStatement( statement, sqlc )

    // Assert
    assertDataFrameEquals( expectedDeduction, result )
  }

  def add100(column: Column): Column = {
    column + 100
  }

  test("testing scala stuffs") {
    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_PrimaryTestData.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_PrimaryTestData.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionMonotonicSeq.csv", sqlc )

    sqlc.udf.register("add100",add100 : Int => Int);

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    val win = Window.partitionBy("line_id").orderBy("sequence_no")

    val layerDeductionJoinedWithLine = layerDeduction.join(line, "layer_id")
    val allFirstDeductions =
      layerDeductionJoinedWithLine
      .withColumn("gross", col("slip_income_amount") * (col("reporting_line_pct")/100))
      .withColumn("lastSequence", lag("sequence_no",1) over win)
      .withColumn("calculateddeductionamount",
        when(col("sequence_no") > 1, null)
          otherwise col("gross") - col("deduction_pct")/100
      )
      .withColumn("currentnet",
        when(col("sequence_no") > 1, null)
           otherwise col("gross") - col("calculatedDeductionAmount")
      )
          .withColumn("netforthissequenceno", col("gross"))
        .collect()

    //SQLRunner.runStatement("Select sequence_no + 100 as d from layer_deduction", sqlc).collect().foreach(println)
    val statement = utils.loadHQLStatementFromResource( "Transformation/DeductionPre2.hql" )

    val result = SQLRunner.runStatement( statement, sqlc )

    result.collect().foreach(println)
  }

  test("testing more scala stuffs") {

    // Arrange
    val sqlc = sqlContext
    sqlContext.sparkContext.setLogLevel( "WARN" )

    // Load test data into dataframes
    val line = populateDataFrameWithLineTestData( "line_MultipleLines.csv", sqlc )
    val layer = populateDataFrameWithLayerTestData( "layer_MultipleLines.csv", sqlc )
    val layerDeduction = populateDataFrameWithLayerDeductionTestData( "layer_deduction_MultipleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    val layerDeductionJoinedWithLine: SchemaRDD = layerDeduction.join(line, "layer_id").repartition(col("line_id")).sortWithinPartitions(col("sequence_no"))

    val expectedDeduction = populateDataFrameWithDeductionTestData( "deduction_MultipleDeductionMonotonicSeqMultipleLines.csv", sqlc )

    val deductions = layerDeductionJoinedWithLine.mapPartitions( iterator => {
      val list: List[Row] = iterator.toList
      val newList = DeductionUtils.getStuff(list)
      newList.iterator
    })

    line.registerTempTable( "line" )
    layer.registerTempTable( "layer" )
    layerDeduction.registerTempTable( "layer_deduction" )

    val deductionSchema = new StructType(Array(
      StructField("line_id",IntegerType, false),
      StructField("deduction_id",IntegerType, false),
      StructField("sequence_no",IntegerType, false),
      StructField("currentnet",StringType, false),
      StructField("calculateddeductionamount",StringType, false)
    ))

    sqlc.createDataFrame(deductions,deductionSchema).registerTempTable("tmpTable")

    val statement = utils.loadHQLStatementFromResource( "Transformation/DeductionTest.hql" )
    val result = SQLRunner.runStatement( statement, sqlc )

    println(result.count())
    assertDataFrameEquals(expectedDeduction,result)

  }


  def add100(value : Int) : Int = {
    value+100
  }

}
