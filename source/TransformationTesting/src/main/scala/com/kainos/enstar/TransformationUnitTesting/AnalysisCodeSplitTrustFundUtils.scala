package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by caoimheb on 12/12/2016.
 */
object AnalysisCodeSplitTrustFundUtils {

  def lineMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ).toInt, null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ) )
  }

  def layerTrustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ) )
  }

  def lookupTrustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ) )
  }

  def analysisCodeSplitMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 6 => Row( null, cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ), cols( 5 ) )
    case _                        => Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ), cols( 4 ), cols( 5 ), cols( 6 ) )
  }

}
