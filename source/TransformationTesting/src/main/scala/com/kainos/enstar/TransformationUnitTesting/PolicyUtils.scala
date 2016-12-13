package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

object PolicyUtils {

  def lineMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      if ( cols( 2 ).equals( "" ) ) null else cols( 2 ),
      if ( cols( 3 ).equals( "" ) ) null else cols( 3 ),
      if ( cols( 4 ).equals( "" ) ) null else cols( 4 ).toInt,
      if ( cols( 5 ).equals( "" ) ) null else cols( 5 ),
      if ( cols( 6 ).equals( "" ) ) null else cols( 6 ),
      if ( cols( 7 ).equals( "" ) ) null else cols( 7 ),
      if ( cols.length > 8 ) if ( cols( 8 ).equals( "" ) ) null else cols( 8 ).toInt else null,
      if ( cols.length > 9 ) if ( cols( 9 ).equals( "" ) ) null else cols( 9 ) else null,
      if ( cols.length > 10 ) if ( cols( 10 ).equals( "" ) ) null else cols( 10 ) else null,
      if ( cols.length > 11 ) if ( cols( 11 ).equals( "" ) ) null else cols( 11 ) else null,
      if ( cols.length > 12 ) if ( cols( 12 ).equals( "" ) ) null else cols( 12 ) else null,
      if ( cols.length > 13 ) if ( cols( 13 ).equals( "" ) ) null else cols( 13 ) else null
    )
  }

  def layerMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ).toInt,
      cols( 1 ).toInt,
      if ( cols.length > 2 ) if ( cols( 2 ).equals( "" ) ) null else cols( 2 ) else null,
      if ( cols.length > 3 ) if ( cols( 3 ).equals( "" ) ) null else cols( 3 ) else null,
      if ( cols.length > 4 ) if ( cols( 4 ).equals( "" ) ) null else cols( 4 ) else null,
      if ( cols.length > 5 ) cols( 5 ) else null
    )
  }

  def submissionMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ).toInt )
  }

  def riskMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ).toInt, cols( 2 ).toInt, cols( 3 ).toInt,
      if ( cols.length > 4 ) cols( 4 ) else null
    )
  }

  def organisationMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt,
      if ( cols.length > 1 ) cols( 1 ) else null
    )
  }

  def lookupBusinessTypeMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def lookupBlockMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ) )
  }

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def underwritingBlockMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ), cols( 1 ), cols( 2 ) )
  }

  def policyMapping( cols : Array[String] ) : Row = {
    Row(
      cols( 0 ),
      cols( 1 ),
      cols( 2 ),
      cols( 3 ),
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
      if ( cols( 17 ).equals( "" ) ) null else cols( 17 ),
      if ( cols( 18 ).equals( "" ) ) null else cols( 18 ),
      if ( cols( 19 ).equals( "" ) ) null else cols( 19 ),
      if ( cols( 20 ).equals( "" ) ) null else cols( 20 ),
      if ( cols( 21 ).equals( "" ) ) null else cols( 21 ),
      if ( cols( 22 ).equals( "" ) ) null else cols( 22 ),
      if ( cols( 23 ).equals( "" ) ) null else cols( 23 ),
      if ( cols( 24 ).equals( "" ) ) null else cols( 24 ),
      if ( cols( 25 ).equals( "" ) ) null else cols( 25 ),
      if ( cols.length > 26 ) if ( cols( 26 ).equals( "" ) ) null else cols( 26 ) else null,
      if ( cols.length > 27 ) if (cols(27).equals("")) null else cols( 27 ).toInt,
      if ( cols.length > 28 ) if ( cols( 28 ).equals( "" ) ) null else cols( 28 ) else null,
      if ( cols.length > 29 ) if ( cols( 29 ).equals( "" ) ) null else cols( 29 ) else null,
      if ( cols.length > 30 ) if ( cols( 30 ).equals( "" ) ) null else cols( 30 ) else null,
      if ( cols.length > 31 ) if ( cols( 31 ).equals( "" ) ) null else cols( 31 ) else null,
      if ( cols.length > 32 ) if ( cols( 32 ).equals( "" ) ) null else cols( 32 ) else null,
      if ( cols.length > 33 ) if ( cols( 32 ).equals( "" ) ) null else cols( 33 ) else null
    )
  }

}