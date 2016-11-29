package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by caoimheb on 28/11/2016.
 */
object PolicyTransactionUtils {

  def layerMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 3 => Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ), null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ), cols( 3 ) )
  }

  def layerTrustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ) )
  }

  def lineMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ).toInt, null, null, null, null )
    case cols if cols.length == 3 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), null, null, null )
    case cols if cols.length == 4 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), cols( 3 ), null, null )
    case cols if cols.length == 5 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), cols( 3 ), cols( 4 ).toInt, null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), cols( 3 ), cols( 4 ).toInt, cols( 5 ) )
  }

  def lineRiskCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ).toInt )
  }

  def lookupPremiumTypeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def settlementScheduleMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 3 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, null, null, null )
    case cols if cols.length == 4 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ), null, null )
    case cols if cols.length == 5 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ), cols( 4 ), null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ), cols( 4 ), cols( 5 ) )
  }

  def policyTransactionMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ), cols( 3 ).toBoolean, cols( 4 ), cols( 5 ), cols( 6 ), cols( 7 ), cols( 8 ),
      cols( 9 ), cols( 10 ), cols( 11 ), cols( 12 ), cols( 13 ), cols( 14 ), cols( 15 ), cols( 16 ), cols( 17 ) )
  }

}
