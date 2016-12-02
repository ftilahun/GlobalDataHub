package com.kainos.enstar.test.TransformationUnitTesting.FILCode

import com.kainos.enstar.TransformationUnitTesting.FILCodeUtils
import org.scalatest.FlatSpec

class FILCodeUtilsTests extends FlatSpec {

  "lookupFilCodeMapping" should "generate a Row" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"

    // Act
    val row = FILCodeUtils.lookupFilCodeMapping( Array( column1, column2 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( "A" ) )
    assert( row.get( 1 ).equals( "B" ) )

  }

  "filCodeMapping" should "generate a Row given 3 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    // Act
    val row = FILCodeUtils.filCodeMapping( Array( column1, column2, column3 ) )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )

  }

}
