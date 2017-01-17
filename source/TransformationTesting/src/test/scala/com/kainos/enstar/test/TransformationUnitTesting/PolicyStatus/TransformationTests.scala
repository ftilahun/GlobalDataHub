package com.kainos.enstar.test.TransformationUnitTesting.PolicyStatus

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ PolicyStatusUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FunSuite

/**
 * Created by caoimheb on 08/12/2016.
 */
class TransformationTests extends FunSuite with DataFrameSuiteBase {

  private val utils = new TransformationUnitTestingUtils

  def populateDataFrameWithLookupLineStatusTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( "/policystatus/input/" + dataFileName ).toString,
      getClass.getResource( "/policystatus/schemas/lookup_line_status.avro" ).toString,
      _.split( "," ),
      PolicyStatusUtils.lookupLineStatusMapping,
      sqlc
    )
  }

  def populateDataFrameWithPolicyStatusTestData( dataFileName : String, sqlc : SQLContext ) : DataFrame = {

    utils.populateDataFrameFromFile(
      getClass.getResource( "/policystatus/output/" + dataFileName ).toString,
      getClass.getResource( "/policystatus/schemas/policystatus.avro" ).toString,
      _.split( "," ),
      PolicyStatusUtils.policyStatusMapping,
      sqlc
    )
  }

  test( "PolicyStatus transformation mapping test" ) {

    // Arrange
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_line_status = this.populateDataFrameWithLookupLineStatusTestData( "lookup_line_status_PrimaryTestData.csv", sqlc )
    val expectedPolicyStatus = this.populateDataFrameWithPolicyStatusTestData( "policystatus_PrimaryTestData.csv", sqlc )

    val hqlStatement = utils.loadHQLStatementFromResource( "Transformation/ndex/PolicyStatus.hql" )

    // Act
    lookup_line_status.registerTempTable( "lookup_line_status" )
    val result = SQLRunner.runStatement( hqlStatement, sqlc )

    // Assert
    assertDataFrameEquals( expectedPolicyStatus, result )

  }

}
