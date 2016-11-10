package com.kainos.enstar.globaldatahub

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext

/**
 * Provides spark and hive contexts to be shared by all test cases.
 */
object TestContexts {

  private val _sc : SparkContext = new SparkContext(
    new SparkConf()
      .setMaster( "local[1]" )
      .setAppName( this.getClass.getSimpleName ) )
  _sc.setLogLevel( "OFF" )
  private val _sqlC : SQLContext = new SQLContext( _sc )

  case class Data( id : Int, value : String )

  def sparkContext = _sc
  def sqlContext = _sqlC

  def dummyData( numItems : Int ) : DataFrame = {
    val list = ( 1 to numItems ).map{ number => Data( number, "value" + number ) }
    TestContexts.sqlContext.createDataFrame( TestContexts.sparkContext.parallelize( list ) )
  }
}
