package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by caoimheb on 23/11/2016.
 */
object PolicyUtils {

  def layerMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ) )
  }

  def lineMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ).toInt, { if ( cols( 2 ).equals( "" ) ) null else cols( 2 ) }, cols( 3 ), cols( 4 ).toInt, cols( 5 ), cols( 6 ), cols( 7 ) )
  }

  def lookupBlockMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def underwritingBlockMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def policyMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ), cols( 5 ), cols( 6 ), cols( 7 ), cols( 8 ),
      cols( 9 ), cols( 10 ), cols( 11 ), null, cols( 13 ), cols( 14 ), cols( 15 ), cols( 16 ), cols( 17 ),
      null, cols( 19 ), cols( 20 ), cols( 21 ), cols( 22 ), cols( 23 ).toInt )
  }

}