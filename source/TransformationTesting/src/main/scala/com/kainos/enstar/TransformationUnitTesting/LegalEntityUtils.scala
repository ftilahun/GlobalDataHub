package com.kainos.enstar.TransformationUnitTesting

import org.apache.spark.sql.Row

/**
 * Created by terences on 21/11/2016.
 */
object LegalEntityUtils {

  def lookupProfitCentreMapping( cols : Array[String] ) : Row = cols match {
    case cols if cols.length == 1 => Row( cols( 0 ).toInt, null )
    case _                        => Row( cols( 0 ).toInt, cols( 1 ) )
  }

  def legalEntityMapping( cols : Array[String] ) : Row = {
    Row( cols( 0 ),
      cols( 1 ),
      if ( cols( 2 ).equals( "" ) ) null else cols( 2 ),
      if ( cols( 3 ).equals( "" ) ) null else cols( 3 ),
      if ( cols( 4 ).equals( "" ) ) null else cols( 4 ),
      cols( 5 ).toBoolean,
      cols( 6 )
    )
  }
}
