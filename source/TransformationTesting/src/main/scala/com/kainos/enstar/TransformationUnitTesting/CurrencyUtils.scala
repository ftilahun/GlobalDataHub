package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by adamf on 23/11/2016.
 */
object CurrencyUtils {
  def lookupCurrencyMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 1 => Row( cols( 0 ), null )
    case _                        => Row( cols( 0 ), cols ( 1 ) )
  }

  def currencyMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 2 => Row( cols( 0 ), cols( 1 ), null )
    case _                        => Row( cols( 0 ), cols( 1 ), cols( 2 ) )
  }
}
