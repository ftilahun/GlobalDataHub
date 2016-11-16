package com.kainos.enstar.globaldatahub

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.joda.time.{ DateTime, Duration }
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._

/**
 * Provides spark and hive contexts to be shared by all test cases.
 */
object TestContexts {

  private val _sc : SparkContext = new SparkContext(
    new SparkConf()
      .setMaster( "local[1]" )
      .setAppName( this.getClass.getSimpleName ) )
  _sc.setLogLevel( "INFO" )
  private val _sqlC : SQLContext = new SQLContext( _sc )

  case class Data( id : Int, value : String )
  case class Control( lastattunitychangeseq : String,
                      starttime : String,
                      endtime : String,
                      attunityrecordcount : Long,
                      attunitytablename : String,
                      directoryname : String )

  def sparkContext = _sc
  def sqlContext = _sqlC

  def dummyData( numItems : Int ) : DataFrame = {
    val list = ( 1 to numItems ).map { number =>
      Data( number, "value" + number )
    }
    TestContexts.sqlContext.createDataFrame(
      TestContexts.sparkContext.parallelize( list ) )
  }

  def changeDummyData( numItems : Int ) : DataFrame = {
    val operations = List( "INSERT", "UPDATE", "DELETE", "BEFOREIMAGE" )
    val changeOperation = udf(
      ( num : Int ) => operations( num % operations.length ) )
    val dateTime = udf( () => "2016-07-12 11:12:32.111" )
    val changeSeq = udf( () => "20160712111232110000000000000000000" )
    val changeMask = udf( ( num : Int ) =>
      if ( num == 9 || num == 10 ) {
        "380"
      } else {
        "7f"
      } )
    val data = dummyData( numItems )
    data
      .withColumn( "_operation", changeOperation( data( "id" ) ) )
      .withColumn( "_timeStamp", dateTime() )
      .withColumn( "_changesequence", changeSeq() )
      .withColumn( "_changemask", changeMask( data( "id" ) ) )
  }

  def generateControlTable( numItems : Int ) : DataFrame = {
    val list = ( 0 until numItems ).map { number =>
      Control(
        DateTimeFormat
          .forPattern( "YYYYMMDDHHmmSShh" )
          .print( new DateTime() ) + numItems.toString.reverse
          .padTo( 19, "0" )
          .mkString( "" )
          .reverse,
        DateTimeFormat
          .forPattern( "YYYY-MM-dd HH:mm:ss.SSS" )
          .print( new DateTime().minus( Duration.standardMinutes( 10 ) ) ),
        DateTimeFormat
          .forPattern( "YYYY-MM-dd HH:mm:ss.SSS" )
          .print( new DateTime() ),
        number,
        "policy",
        "/some/dir/" + DateTimeFormat
          .forPattern( "YYYYMMddHHmmssSSS" )
          .print( new DateTime() )
      )
    }
    TestContexts.sqlContext.createDataFrame(
      TestContexts.sparkContext.parallelize( list ) )
  }

}
