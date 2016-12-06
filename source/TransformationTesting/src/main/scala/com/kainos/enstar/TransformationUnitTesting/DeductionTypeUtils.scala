package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by adamf on 30/11/2016.
 */
object DeductionTypeUtils {
  def lookupDeductionTypeMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 2 => Row( cols( 0 ), cols( 1 ), null )
    case _                        => Row( cols( 0 ), cols( 1 ), cols ( 2 ) )
  }

  def deductionTypeMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 3 => Row( cols( 0 ), cols( 1 ), cols( 2 ), null )
    case _                        => Row( cols( 0 ), cols( 1 ), cols( 2 ), cols ( 3 ) )
  }
}
