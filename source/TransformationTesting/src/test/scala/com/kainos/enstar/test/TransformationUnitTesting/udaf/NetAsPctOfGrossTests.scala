package com.kainos.enstar.test.TransformationUnitTesting.udaf

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting._
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

/**
  * Created by terences on 15/12/2016.
  */
class NetAsPctOfGrossTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  test("NetAsPctOfGross aggregation with monotonic increasing sequence with multiple sequence number groups") {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/input/input_MontonicSeqMultipleSeqGroups.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/input.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.inputMapping,
      sqlc
    )

    // Load Expected Results into dataframe
    val expectedCurrencyMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/output/output_MonotonicSeqMultipleSeqGroups.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/output.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.outputMapping,
      sqlc
    )

    val statement = utils.loadHQLStatementFromResource("UDAF/NetAsPctOfGross.hql")

    sqlc.udf.register("net_as_pct_of_gross", NetAsPctOfGross);
    lookup_currency.registerTempTable( "input" )

    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedCurrencyMapping.orderBy("id"), result.orderBy("id") )
  }

  test("NetAsPctOfGross aggregation with non monotonic increasing sequence with multiple sequence number groups") {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/input/input_NonMontonicSeqMultipleSeqGroups.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/input.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.inputMapping,
      sqlc
    )

    // Load Expected Results into dataframe
    val expectedCurrencyMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/output/output_NonMonotonicSeqMultipleSeqGroups.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/output.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.outputMapping,
      sqlc
    )

    val statement = utils.loadHQLStatementFromResource("UDAF/NetAsPctOfGross.hql")

    sqlc.udf.register("net_as_pct_of_gross", NetAsPctOfGross);
    lookup_currency.registerTempTable( "input" )

    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedCurrencyMapping.orderBy("id"), result.orderBy("id") )
  }

  test("NetAsPctOfGross aggregation with non monotonic increasing sequence with multiple sequence number groups and unordered input") {

    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/input/input_NonMontonicSeqMultipleSeqGroupsUnordered.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/input.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.inputMapping,
      sqlc
    )

    // Load Expected Results into dataframe
    val expectedCurrencyMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/output/output_NonMonotonicSeqMultipleSeqGroupsUnordered.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/output.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.outputMapping,
      sqlc
    )

    val statement = utils.loadHQLStatementFromResource("UDAF/NetAsPctOfGross.hql")

    sqlc.udf.register("net_as_pct_of_gross", NetAsPctOfGross);
    lookup_currency.registerTempTable( "input" )

    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedCurrencyMapping.orderBy("id"), result.orderBy("id") )
  }

  test("NetAsPctOfGross aggregation with multiple LayerIds") {

    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_currency : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/input/input_MultipleLayerIds.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/input.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.inputMapping,
      sqlc
    )

    // Load Expected Results into dataframe
    val expectedCurrencyMapping : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource("/UDAF/NetAsPctOfGross/output/output_MultipleLayerIds.csv").toString,
      getClass.getResource("/UDAF/NetAsPctOfGross/schemas/output.avro").toString,
      _.split( "," ),
      NetAsPctOfGrossUtils.outputMapping,
      sqlc
    )

    val statement = utils.loadHQLStatementFromResource("UDAF/NetAsPctOfGross.hql")

    sqlc.udf.register("net_as_pct_of_gross", NetAsPctOfGross);
    lookup_currency.registerTempTable( "input" )

    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedCurrencyMapping.orderBy("id"), result.orderBy("id") )
  }
}
