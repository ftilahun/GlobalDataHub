package com.kainos.enstar.transformation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.kainos.enstar.TransformationUnitTesting.{ NetAsPctOfGross, TransformationUnitTestingUtils }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.reflections.Reflections
import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import com.kainos.enstar.transformation.tags._
import com.kainos.enstar.transformation.udf.TransformationUdfRegistry

import collection.JavaConverters._

/**
 * Created by neilri on 10/01/2017.
 */
class AllTransformationTests extends FunSuite with DataFrameSuiteBase with BeforeAndAfterAll {

  val reflections = new Reflections( "com.kainos.enstar.transformation" )

  val querySuiteClasses = reflections.getSubTypesOf( classOf[QuerySuite] ).asScala

  // must be def rather than val because sqlContext is null during instantiation
  implicit def sqlc = sqlContext

  val utils = new TransformationUnitTestingUtils

  override def beforeAll() : Unit = {
    super.beforeAll()
    sqlContext.sparkContext.setLogLevel( "WARN" )
    TransformationUdfRegistry.registerUdfs( sqlContext )
  }

  for {
    querySuiteClass <- querySuiteClasses
    queryTestSet <- querySuiteClass.newInstance().queryTestSets
  } {
    val querySuite : QuerySuite = querySuiteClass.newInstance()

    transformationTests( querySuite, queryTestSet )
    reconciliationTests( querySuite, queryTestSet )
  }

  def transformationTests( querySuite : QuerySuite, queryTestSet : QueryTestSet ) : Unit = {
    val testTags = querySuite.testTags ++ querySuite.sourceType.testTags :+ Transformation

    for ( queryTest <- queryTestSet.queryTests ) {
      test( s"Transformation - ${querySuite.sourceType.packageName} - ${queryTestSet.name} - ${queryTest.name}", testTags.toArray : _* ) {
        // Arrange
        prepareContext( querySuite, queryTestSet, queryTest.sourceDataSet )

        // Act
        val expected = utils.populateDataFrameFromCsvWithHeader( expectedCsvSource( querySuite, queryTestSet, queryTest ) )
        val statement = loadTransformationQuery( querySuite, queryTestSet )
        val result = sqlc.sql( statement )

        val orderedExpected = orderDataFrame( expected, queryTest.order )
        val orderedResult = orderDataFrame( result, queryTest.order )

        // Assert
        assertDataFrameEquals( orderedExpected, orderedResult )

        // Reset
        resetContext( queryTest.sourceDataSet )
      }
    }
  }

  def reconciliationTests( querySuite : QuerySuite, queryTestSet : QueryTestSet ) : Unit = {
    val testTags = querySuite.testTags ++ querySuite.sourceType.testTags :+ Reconciliation

    for ( reconciliationTest <- queryTestSet.reconciliationTests ) {
      test( s"Reconciliation - ${querySuite.sourceType.packageName}  - ${queryTestSet.name} - ${reconciliationTest.name}", testTags.toArray : _* ) {
        // Arrange
        prepareContext( querySuite, queryTestSet, reconciliationTest.sourceDataSet )

        // Act
        val statement = loadTransformationQuery( querySuite, queryTestSet )
        val reconSourceStatement = loadReconciliationQuery( querySuite.sourceType.packageName, reconciliationTest.sourceQuerySource )
        val reconDestStatement = loadReconciliationQuery( "ecm", reconciliationTest.destQuerySource )

        val result = sqlc.sql( statement )
        val reconSourceResult = sqlc.sql( reconSourceStatement )

        // force run of query to deal with cases where ECM table name is same as source table name
        reconSourceResult.rdd.cache()

        result.registerTempTable( reconciliationTest.destTableName )

        val reconDestResult = sqlc.sql( reconDestStatement )

        // Assert
        assertDataFrameEquals( reconSourceResult, reconDestResult )

        // Reset
        resetContext( reconciliationTest.sourceDataSet )
      }
    }
  }

  def prepareContext( querySuite : QuerySuite, queryTestSet : QueryTestSet, sourceDataSet : Set[CsvSourceData] ) : Unit = {
    for ( sourceData <- sourceDataSet ) {
      utils.populateDataFrameFromCsvWithHeader( inputCsvSource( querySuite, queryTestSet, sourceData ) ).registerTempTable( sourceData.tableName )
    }
  }

  def resetContext( sourceDataSet : Set[CsvSourceData] ) : Unit = {
    for ( sourceData <- sourceDataSet ) {
      sqlc.dropTempTable( sourceData.tableName )
    }
    sqlc.clearCache()
  }

  def loadTransformationQuery( querySuite : QuerySuite, queryTestSet : QueryTestSet ) : String = {
    utils.loadHQLStatementFromResource( s"Transformation/${querySuite.sourceType.packageName}/${queryTestSet.querySource}" )
  }

  def loadReconciliationQuery( packageName : String, source : String ) : String = {
    utils.loadHQLStatementFromResource( s"Reconciliation/${packageName}/${source}" )
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
