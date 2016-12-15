package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by terences on 23/11/2016.
 */
object AnalysisCodeSplitRiskCodeUtils {

  def lineMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ).toInt, null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ) )
  }

  def lineRiskCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ) )
  }

  def lookupRiskCodeMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ),
      if ( cols( 1 ).equals( "" ) ) null else cols( 1 ) )
  }

  def analysisCodeSplitMapping( cols : Array[String] ) : Row = {
    Row(
      if ( cols( 0 ).equals( "" ) ) null else cols( 0 ),
      cols( 1 ),
      cols( 2 ),
      cols( 3 ),
      cols( 4 ),
      cols( 5 ),
      if ( cols( 6 ).equals( "" ) ) null else cols( 6 ),
      cols( 7 )
    )
    //case cols if cols.length == 6 => Row( null, cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ), cols( 5 ) )
    //case _                        => Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ), cols( 5 ), cols( 6 ) )
  }
}
