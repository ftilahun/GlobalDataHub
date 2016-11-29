package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by adamf on 29/11/2016.
 */
object GeographyTypeUtils {

  def lookupGeographyTypeMapping( cols : Array[String] ) : Row = cols match {
    case _ => Row( null )
  }

  def geographyMapping( cols : Array[String] ) : Row = cols match {
    case _ => Row( cols( 0 ), cols( 1 ), cols( 2 ) )
  }
}
