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

  def lineriskcodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ) )
  }
}
