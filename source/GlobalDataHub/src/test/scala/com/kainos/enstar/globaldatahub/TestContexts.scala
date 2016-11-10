package com.kainos.enstar.globaldatahub

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.{ DateTime, Duration }
import org.joda.time.format.DateTimeFormat

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
  case class Control(
    lastattunitychangeseq : String,
    starttime : String,
    endtime : String,
    attunityrecordcount : Long,
    attunitytablename : String,
    directoryname : String )

  def sparkContext = _sc
  def sqlContext = _sqlC

  def dummyData( numItems : Int ) : DataFrame = {
    val list = ( 1 to numItems ).map{ number => Data( number, "value" + number ) }
    TestContexts.sqlContext.createDataFrame( TestContexts.sparkContext.parallelize( list ) )
  }

  def generateControlTable( numItems : Int ) : DataFrame = {
    val list = ( 1 to numItems ).map{ number =>
      Control(
        DateTimeFormat.forPattern( "YYYYMMDDHHmmSShh" ).print( new DateTime() ) + numItems.toString.reverse.padTo( 19, "0" ).mkString( "" ).reverse,
        DateTimeFormat.forPattern( "YYYY-MM-dd HH:mm:ss.SSS" ).print( new DateTime().minus( Duration.standardMinutes( 10 ) ) ),
        DateTimeFormat.forPattern( "YYYY-MM-dd HH:mm:ss.SSS" ).print( new DateTime() ),
        number,
        "policy",
        "/some/dir/" + DateTimeFormat.forPattern( "YYYYMMddHHmmssSSS" ).print( new DateTime() )
      )
    }
    TestContexts.sqlContext.createDataFrame( TestContexts.sparkContext.parallelize( list ) )
  }

}
