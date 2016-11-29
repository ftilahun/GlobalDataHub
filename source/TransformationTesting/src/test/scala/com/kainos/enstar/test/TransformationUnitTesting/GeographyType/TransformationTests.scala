package com.kainos.enstar.test.TransformationUnitTesting.GeographyType

/**
 * Created by adamf on 29/11/2016.
 */

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ GeographyTypeUtils, SQLRunner, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class TransformationTests extends FunSuite with DataFrameSuiteBase {

  test( "GeographyType_test1" ){
    val sqlc = sqlContext
    val utils = new TransformationUnitTestingUtils

    val lookup_geographytype : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/geographytype/input/geographytypeinput_test1.csv" ).toString,
      getClass.getResource( "/geographytype/schemas/lookup_geographytype.avro" ).toString,
      _.split( "," ),
      GeographyTypeUtils.lookupGeographyTypeMapping,
      sqlContext
    )

    val expectedGeographyType : DataFrame = utils.populateDataFrameFromFile(
      getClass.getResource( "/geographytype/output/geographytypeoutput_test1.csv" ).toString,
      getClass.getResource( "/geographytype/schemas/geographytype.avro" ).toString,
      _.split( "," ),
      GeographyTypeUtils.geographyMapping,
      sqlContext
    )

    val statement = utils.loadHQLStatementFromResource( "Transformation/GeographyType.hql" )

    lookup_geographytype.registerTempTable( "lookup_geographytype" )
    val result = SQLRunner.runStatement( statement, sqlc )

    assertDataFrameEquals( expectedGeographyType, result )
  }
}
