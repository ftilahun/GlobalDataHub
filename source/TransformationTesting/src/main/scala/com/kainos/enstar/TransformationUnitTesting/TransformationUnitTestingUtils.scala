package com.kainos.enstar.TransformationUnitTesting

import java.io.InputStream

import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

/**
 * Created by terences on 19/11/2016.
 */
class TransformationUnitTestingUtils {

  def populateDataFrameFromFile( dataResourceLocation : String, avroSchemaResourceLocation : String, fromStringArraytoRow : Array[String] => Row, sqlContext : SQLContext ) : DataFrame = {

    val dataRowRDD = loadRDDFromFile( dataResourceLocation, sqlContext ) map splitStringToArray map fromStringArraytoRow

    val schema = loadSchemaFromFile( avroSchemaResourceLocation, sqlContext )

    val dataFrame = sqlContext.createDataFrame( dataRowRDD, schema )

    dataFrame
  }

  def splitStringToArray(stringToSplit: String): Array[String] ={
    stringToSplit.split(",")
  }

  def loadHQLStatementFromResource( filename : String )() : String = {
    val stream : InputStream = getClass.getResourceAsStream( "/" + filename )
    val lines = scala.io.Source.fromInputStream( stream ).mkString
    stream.close()
    lines
  }

  def loadRDDFromFile( dataResourceLocation : String, sqlContext : SQLContext ) : RDD[String] = {
    sqlContext.sparkContext.textFile( dataResourceLocation )
  }

  def loadSchemaFromFile( avroSchemaResourceLocation : String, sqlContext : SQLContext ) : StructType = {
    sqlContext.read.avro( avroSchemaResourceLocation ).schema
  }

}
