package com.kainos.enstar.TransformationUnitTesting

import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

/**
 * Created by terences on 19/11/2016.
 */
class TransformationUnitTestingUtils {

  def populateDataFrameFromFile( dataResourceLocation : String, avroSchemaResourceLocation : String, mapping : Array[String] => Row, sqlContext : SQLContext ) : DataFrame = {

    val dataRowRDD = loadRDDFromFile( dataResourceLocation, sqlContext )
      .map( _.split( "," ) )
      .map( col => mapping( col ) )

    val schema = loadSchemaFromFile( avroSchemaResourceLocation, sqlContext )

    val dataFrame = sqlContext.createDataFrame( dataRowRDD, schema )

    dataFrame
  }

  def loadRDDFromFile( dataResourceLocation : String, sqlContext : SQLContext ) : RDD[String] = {
    sqlContext.sparkContext.textFile( dataResourceLocation )
  }

  def loadSchemaFromFile( avroSchemaResourceLocation : String, sqlContext : SQLContext ) : StructType = {
    sqlContext.read.avro( avroSchemaResourceLocation ).schema
  }

}
