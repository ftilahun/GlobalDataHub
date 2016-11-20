package com.kainos.enstar.testing

import com.databricks.spark.avro._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

/**
 * Created by terences on 19/11/2016.
 */
object testing {
  /*
  val sc =
    new SparkContext(
      new SparkConf()
        .setMaster( "local[1]" )
        .setAppName( this.getClass.getSimpleName ) )
  sc.setLogLevel( "OFF" )

  val sqlc = new SQLContext( sc )
*/
  case class Lookup( profit_centre_code : Int, profit_centre_desc : String )
  /*
  def populate2() : DataFrame = {
    val file : URL = getClass.getResource( "/lookup_profit_centre.csv" )
    val listRDD = sc.textFile( file.toString )
      .map( _.split( "," ) )
      .map( col => new Lookup( col( 0 ).toInt, col( 1 ) ) with Serializable )

    println( listRDD )
    listRDD.collect().foreach( println )

    val dataFrame : DataFrame = sqlc.createDataFrame( listRDD )
    dataFrame
  }

  def populate() : DataFrame = {

    val file : URL = getClass.getResource( "/lookup_profit_centre.csv" )
    val listRDD = sc.textFile( file.toString )
      .map( _.split( "," ) )
      .map( col => Row( col( 0 ).toInt, col( 1 ) ) )

    println( listRDD )
    listRDD.collect().foreach( println )

    val schematype = StructType(
      StructField( "profit_centre_code", org.apache.spark.sql.types.IntegerType, false ) :: StructField( "profit_centre_code", org.apache.spark.sql.types.StringType, false ) :: Nil
    )

    println( lookup_profit_centre.getClassSchema.toString )

    val dataFrame : DataFrame = sqlc.createDataFrame( listRDD, schematype )
    dataFrame
  }
*/

  def populateDataFrame( dataResourceLocation : String, avroSchemaResourceLocation : String, mapping : Array[String] => Row, sparkContext : SparkContext, sqlContext : SQLContext ) : DataFrame = {

    val listRDD : RDD[Row] = sparkContext.textFile( dataResourceLocation )
      .map( _.split( "," ) )
      .map( col => mapping( col ) )

    val schemaAvro = sqlContext.read.avro( avroSchemaResourceLocation ).schema

    val dataFrame : DataFrame = sqlContext.createDataFrame( listRDD, schemaAvro )
    dataFrame
  }
}
