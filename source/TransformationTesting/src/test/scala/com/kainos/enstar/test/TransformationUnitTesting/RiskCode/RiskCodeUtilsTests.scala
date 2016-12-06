package com.kainos.enstar.test.TransformationUnitTesting.RiskCode

/**
 * Created by adamf on 30/11/2016.
 */

import com.kainos.enstar.TransformationUnitTesting.RiskCodeUtils
import org.scalatest.FlatSpec

class RiskCodeUtilsTests extends FlatSpec {
  "lookupRiskCodeMapping" should "generate a Row" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"

    // Act
    val row = RiskCodeUtils.lookupRiskCodeMapping( Array( column1, column2 ) )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( "A" ) )
    assert( row.get( 1 ).equals( "B" ) )

  }

  "riskCodeMapping" should "generate a Row" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    // Act
    val row = RiskCodeUtils.riskCodeMapping( Array( column1, column2, column3 ) )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( "A" ) )
    assert( row.get( 1 ).equals( "B" ) )
    assert( row.get( 2 ).equals( "C" ) )
  }
}