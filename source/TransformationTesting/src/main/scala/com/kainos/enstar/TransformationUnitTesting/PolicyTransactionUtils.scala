package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

object PolicyTransactionUtils {

  def layerMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      if ( cols.length > 1 ) if ( cols( 1 ).equals( "" ) ) null else cols( 1 ) else null,
      if ( cols.length > 2 ) if ( cols( 2 ).equals( "" ) ) null else cols( 2 ) else null,
      if ( cols.length > 3 ) if ( cols( 3 ).equals( "" ) ) null else cols( 3 ) else null
    )
  }

  def layerTrustFundMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      if ( cols.length > 1 ) if ( cols( 1 ).equals( "" ) ) null else cols( 1 ) else null,
      if ( cols.length > 2 ) if ( cols( 1 ).equals( "" ) ) null else cols( 2 ) else null
    )
  }

  def lineMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      if ( cols.length > 2 ) if ( cols( 2 ).equals( "" ) ) null else cols( 2 ) else null,
      if ( cols.length > 3 ) if ( cols( 3 ).equals( "" ) ) null else cols( 3 ) else null,
      if ( cols.length > 4 ) if ( cols( 4 ).equals( "" ) ) null else cols( 4 ).toInt else null,
      if ( cols.length > 5 ) if ( cols( 5 ).equals( "" ) ) null else cols( 5 ) else null
    )
  }

  def lineRiskCodeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ).toInt )
  }

  def lookupPremiumTypeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def settlementScheduleMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      cols( 2 ).toInt,
      if ( cols.length > 3 ) if ( cols( 3 ).equals( "" ) ) null else cols( 3 ) else null,
      if ( cols.length > 4 ) if ( cols( 4 ).equals( "" ) ) null else cols( 4 ) else null,
      if ( cols.length > 5 ) if ( cols( 5 ).equals( "" ) ) null else cols( 5 ) else null
    )
  }

  def policyTransactionMapping( cols : Array[String] ) : Row = Row(
    if ( cols( 0 ).equals( "" ) ) null else cols( 0 ),
    if ( cols( 1 ).equals( "" ) ) null else cols( 1 ),
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
