package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
  * Created by terences on 15/12/2016.
  */
object NetAsPctOfGrossUtils {

  def inputMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ), cols(2).toInt, cols(3) )
  }

  def outputMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols(1), cols(2) )
  }

}
