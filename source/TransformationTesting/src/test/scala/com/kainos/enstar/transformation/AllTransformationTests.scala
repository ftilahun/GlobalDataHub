package com.kainos.enstar.transformation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ NetAsPctOfGross, TransformationUnitTestingUtils }
import org.apache.spark.sql.DataFrame
import org.reflections.Reflections
import org.scalatest.FunSuite

import collection.JavaConverters._

/**
 * Created by neilri on 10/01/2017.
 */
class AllTransformationTests extends FunSuite with DataFrameSuiteBase {

  val reflections = new Reflections( "com.kainos.enstar.transformation" )

  val querySuiteClasses = reflections.getSubTypesOf( classOf[QuerySuite] ).asScala

  // must be def rather than val because sqlContext is null during instantiation
  implicit def sqlc = sqlContext

  val utils = new TransformationUnitTestingUtils

  for {
    querySuiteClass <- querySuiteClasses
    queryTestSet <- querySuiteClass.newInstance().queryTestSets
    queryTest <- queryTestSet.tests
  } {
    val querySuite : QuerySuite = querySuiteClass.newInstance()

    val testTags = querySuite.testTags ++ querySuite.sourceType.testTags

    test( s"${queryTestSet.name} - ${queryTest.name}", testTags.toArray : _* ) {
      sqlContext.sparkContext.setLogLevel( "WARN" )

      // Arrange
      for ( sourceData <- queryTest.sourceDataSet ) {
        utils.populateDataFrameFromCsvWithHeader( inputCsvSource( querySuite, queryTestSet, sourceData ) ).registerTempTable( sourceData.tableName )
      }
      sqlc.udf.register( "net_as_pct_of_gross", NetAsPctOfGross )

      // Act
      val expected = utils.populateDataFrameFromCsvWithHeader( expectedCsvSource( querySuite, queryTestSet, queryTest ) )
      val statement = utils.loadHQLStatementFromResource( s"Transformation/${querySuite.sourceType.packageName}/${queryTestSet.querySource}" )
      val result = sqlc.sql( statement )

      val orderedExpected = orderDataFrame( expected, queryTest.order )
      val orderedResult = orderDataFrame( result, queryTest.order )

      // Assert
      assertDataFrameEquals( orderedExpected, orderedResult )
    }
  }

  def inputCsvSource( querySuite : QuerySuite, queryTestSet : QueryTestSet, sourceData : CsvSourceData ) : String = {
    s"/${querySuite.sourceType.packageName}/${queryTestSet.baseDir}/input/${sourceData.tableName}/${sourceData.source}"
  }

  def expectedCsvSource( querySuite : QuerySuite, queryTestSet : QueryTestSet, queryTest : QueryTest ) : String = {
    s"/${querySuite.sourceType.packageName}/${queryTestSet.baseDir}/output/${queryTest.expectedResult.source}"
  }

  def orderDataFrame( df : DataFrame, order : List[String] ) : DataFrame = order match {
    case List()        => df
    case List( field ) => df.orderBy( field )
    case field :: tail => df.orderBy( field, tail : _* )
  }
}
