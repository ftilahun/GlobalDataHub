package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by sionam on 02/12/2016.
 */
object PolicyTransactionDeductionsUtils {

  def lineMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 5 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), cols( 3 ).toInt, cols( 4 ), null )
    case cols if cols.length == 4 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), cols( 3 ).toInt, null, null )
    case cols if cols.length == 3 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), null, null, null )
    case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ).toInt, null, null, null, null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ), cols( 3 ).toInt, cols( 4 ), cols( 5 ) )
  }

  def layerMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 3 => Row( cols( 0 ).toInt, cols( 1 ), cols(2), null)
    case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ), null, null)
    case cols if cols.length == 1 => Row( cols( 0 ).toInt, null, null, null)
    case _                        => Row( cols( 0 ).toInt, cols( 1 ), cols( 2 ), cols(3) )
  }

  def layerDeductionMapping( cols : Array[String] ) : Row = {
    cols match {
      case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ).toInt, null )
      case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ) )
    }
  }

  def layerTrustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ).toInt )
  }

  def lineRiskCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ).toInt )
  }

  def lookupDeductionTypeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def lookupRiskCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ) )
  }

  def settlementScheduleMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 4 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ), null )
    case cols if cols.length == 3 => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, null, null )
    case cols if cols.length == 2 => Row( cols( 0 ).toInt, cols( 1 ).toInt, null, null, null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ), cols( 4 ) )
  }

  def lookupTrustFundMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ) )
  }

  def policyTransactionDeductionsMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ),
      if ( cols( 2 ).equals( "" ) ) null else cols( 2 ),
      if ( cols( 3 ).equals( "" ) ) null else cols( 3 ).toBoolean,
      if ( cols( 4 ).equals( "" ) ) null else cols( 4 ),
      if ( cols( 5 ).equals( "" ) ) null else cols( 5 ),
      if ( cols( 6 ).equals( "" ) ) null else cols( 6 ),
      if ( cols( 7 ).equals( "" ) ) null else cols( 7 ),
      if ( cols( 8 ).equals( "" ) ) null else cols( 8 ),
      if ( cols( 9 ).equals( "" ) ) null else cols( 9 ),
      if ( cols( 10 ).equals( "" ) ) null else cols( 10 ),
      if ( cols( 11 ).equals( "" ) ) null else cols( 11 ),
      if ( cols( 12 ).equals( "" ) ) null else cols( 12 ),
      if ( cols( 13 ).equals( "" ) ) null else cols( 13 ),
      if ( cols( 14 ).equals( "" ) ) null else cols( 14 ),
      if ( cols( 15 ).equals( "" ) ) null else cols( 15 ),
      if ( cols( 16 ).equals( "" ) ) null else cols( 16 ),
      if ( cols( 17 ).equals( "" ) ) null else cols( 17 )
    )
  }

}