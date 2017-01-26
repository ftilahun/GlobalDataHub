package com.kainos.enstar.transformation.runner

import com.kainos.enstar.TransformationUnitTesting.SQLRunner
import com.kainos.enstar.transformation.udf.TransformationUdfRegistry
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import com.kainos.enstar.TransformationUnitTesting.TransformationUnitTestingUtils

class TransformationContext() {

  val conf = new SparkConf().setAppName( "Transformation Runner" )
  val sc = new SparkContext( conf )

  val hc = new HiveContext( sc )
  hc.setConf( "spark.sql.avro.compression.codec", "uncompressed" )

  TransformationUdfRegistry.registerUdfs( hc )

  def setSourceDB( sourceDB : String ) = {
    val setDBStatement = "USE " + sourceDB
    SQLRunner.runStatement( setDBStatement, hc )
  }

  def runTransformation( transformationFileName : String ) : DataFrame = {
    val transformationUtils = new TransformationUnitTestingUtils
    val hqlStatement = transformationUtils.loadHQLStatementFromResource( transformationFileName )
    SQLRunner.runStatement( hqlStatement, hc )
  }
}
