package com.kainos.enstar.TransformationUnitTesting

import com.databricks.spark.avro._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

/**
 * Created by terences on 19/11/2016.
 */
object TransformationUnitTestingUtils {

  def populateDataFrame( dataResourceLocation : String, avroSchemaResourceLocation : String, mapping : Array[String] => Row, sqlContext : SQLContext ) : DataFrame = {

    val dataRDD : RDD[Row] = sqlContext.sparkContext.textFile( dataResourceLocation )
      .map( _.split( "," ) )
      .map( col => mapping( col ) )

    val schema = sqlContext.read.avro( avroSchemaResourceLocation ).schema

    val dataFrame : DataFrame = sqlContext.createDataFrame( dataRDD, schema )
    dataFrame
  }
}
