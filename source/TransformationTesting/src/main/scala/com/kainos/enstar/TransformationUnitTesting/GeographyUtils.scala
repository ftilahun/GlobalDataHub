package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by adamf on 29/11/2016.
 */
object GeographyUtils {
  def lookupCountryMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols ( 2 ) )
  }

  def geographyMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ), cols( 5 ), cols( 6 ), cols( 7 ) )
  }
}
