package com.kainos.enstar.TransformationUnitTesting

import java.io.InputStream

import com.databricks.spark.avro._
import com.kainos.enstar.transformation.{ CsvReader, Schema, StructuredCsvReader }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

import scala.io.Source

/**
 * Created by terences on 19/11/2016.
 */
class TransformationUnitTestingUtils {

  def populateDataFrameFromFile( dataResourceLocation : String, avroSchemaResourceLocation : String,
                                 fromInputDataRowToStringArray : String => Array[String],
                                 fromStringArraytoRow : Array[String] => Row,
                                 sqlContext : SQLContext ) : DataFrame = {

    val dataRowRDD = loadRDDFromFile( dataResourceLocation )( sqlContext )
      .map( fromInputDataRowToStringArray ).map( fromStringArraytoRow )

    val schema = loadSchemaFromFile( avroSchemaResourceLocation, sqlContext )

    val dataFrame = sqlContext.createDataFrame( dataRowRDD, schema )

    dataFrame
  }

  def populateDataFrameFromCsvWithHeader( dataResourceName : String )( implicit sqlContext : SQLContext ) : DataFrame = {
    val dataResourceLocation = getClass.getResource( dataResourceName )

    val reader = StructuredCsvReader( dataResourceLocation )

    val rows = sqlContext.sparkContext.makeRDD( reader.rows )
    sqlContext.createDataFrame( rows, reader.schema.structType )
  }

  def loadHQLStatementFromResource( filename : String )() : String = {
    val stream : InputStream = getClass.getResourceAsStream( "/" + filename )
    val lines = scala.io.Source.fromInputStream( stream ).mkString
    stream.close()
    lines
  }

  def loadRDDFromFile( dataResourceLocation : String )( implicit sqlContext : SQLContext ) : RDD[String] = {
    sqlContext.sparkContext.textFile( dataResourceLocation )
  }

  def loadSchemaFromFile( avroSchemaResourceLocation : String, sqlContext : SQLContext ) : StructType = {
    sqlContext.read.avro( avroSchemaResourceLocation ).schema
  }

}
