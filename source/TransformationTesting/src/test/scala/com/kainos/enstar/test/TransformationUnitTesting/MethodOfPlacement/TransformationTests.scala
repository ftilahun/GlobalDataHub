package com.kainos.enstar.test.TransformationUnitTesting.MethodOfPlacement

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{MethodOfPlacementUtils, SQLRunner, TransformationUnitTestingUtils}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

/**
  * Created by caoimheb on 08/12/2016.
  */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLookupBusinessTypeTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( "/methodofplacement/input/" + dataFileName ).toString,
      getClass.getResource( "/methodofplacement/schemas/lookup_business_type.avro" ).toString,
      _.split( "," ),
      MethodOfPlacementUtils.lookupBusinessTypeMapping,
      sqlc
    )
  }

  def populateDataFrameWithMethodOfPlacementTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( "/methodofplacement/output/" + dataFileName ).toString,
      getClass.getResource( "/methodofplacement/schemas/methodofplacement.avro" ).toString,
      _.split( "," ),
      MethodOfPlacementUtils.methodOfPlacementMapping,
      sqlc
    )
  }

  test( "TrustFund transformation mapping test" ) {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_business_type = this.populateDataFrameWithLookupBusinessTypeTestData( "lookup_business_type_PrimaryTestData.csv", sqlc )
    val expectedMethodOfPlacement = this.populateDataFrameWithMethodOfPlacementTestData( "methodofplacement_PrimaryTestData.csv", sqlc )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/MethodOfPlacement.hql" )

    // Act
    lookup_business_type.registerTempTable( "lookup_business_type" )
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedMethodOfPlacement, result )

  }

}
