package com.kainos.enstar.test.TransformationUnitTesting.Geography

import com.kainos.enstar.TransformationUnitTesting.GeographyUtils
import org.scalatest.FlatSpec

/**
 * Created by adamf on 29/11/2016.
 */

class GeographyUtilsTests extends FlatSpec {
  "lookupCountryMapping" should "generate a Row" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"

    // Act
    val row = GeographyUtils.lookupCountryMapping( Array( column1, column2 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )

  }

  "geographyMapping" should "generate a Row" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"
    val column5 = "E"
    val column6 = "F"
    val column7 = "G"
    val column8 = "H"

    // Act
    val row = GeographyUtils.geographyMapping( Array( column1, column2, column3, column4, column5, column6, column7, column8 ) )

    // Assert
    assert( row.size == 8 )
    assert( row.get( 0 ).equals( "A" ) )
    assert( row.get( 1 ).equals( "B" ) )
    assert( row.get( 2 ).equals( "C" ) )
    assert( row.get( 3 ).equals( "D" ) )
    assert( row.get( 4 ).equals( "E" ) )
    assert( row.get( 5 ).equals( "F" ) )
    assert( row.get( 6 ).equals( "G" ) )
    assert( row.get( 7 ).equals( "H" ) )
  }
}