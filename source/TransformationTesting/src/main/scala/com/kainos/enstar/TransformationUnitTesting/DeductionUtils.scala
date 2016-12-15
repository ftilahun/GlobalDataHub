package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
 * Created by terences on 08/12/2016.
 */
object DeductionUtils {

  def lineMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      cols( 2 ),
      cols( 3 ),
      cols( 4 ),
      cols( 5 )
    )
  }

  def layerMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 )
    )
  }

  def layerDeductionMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      cols( 2 ).toInt,
      cols( 3 ),
      cols( 4 )
    )
  }

  def deductionMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ),
      cols( 1 ),
      cols( 2 ),
      cols( 3 ),
      cols( 4 ),
      cols( 5 ),
      cols( 6 ),
      cols( 7 ),
      cols( 8 ),
      cols( 9 ),
      cols( 10 ).toBoolean,
      cols( 11 ).toBoolean
    )
  }
}
