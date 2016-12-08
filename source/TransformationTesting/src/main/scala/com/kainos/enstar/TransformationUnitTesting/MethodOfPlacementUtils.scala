package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
  * Created by caoimheb on 08/12/2016.
  */
object MethodOfPlacementUtils {

  def lookupBusinessTypeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def methodOfPlacementMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ) )
  }

}
