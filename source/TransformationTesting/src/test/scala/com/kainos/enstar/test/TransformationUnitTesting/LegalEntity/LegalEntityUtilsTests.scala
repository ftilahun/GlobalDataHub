package com.kainos.enstar.test.TransformationUnitTesting.LegalEntity

import com.kainos.enstar.TransformationUnitTesting.LegalEntityUtils
import org.scalatest.FlatSpec

/**
 * Created by terences on 21/11/2016.
 */
class LegalEntityUtilsTests extends FlatSpec {

  "lookupProfitCentreMapping" should "generate a Row" in {

    // Arrange
    val column1 = "1"
    val column2 = "B"

    // Act
    val row = LegalEntityUtils.lookupProfitCentreMapping( ( column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column1.toInt )
    assert( row.get( 1 ).equals( column2 ) )
  }

  "lookupProfitCentreMapping" should "generate a row given 1 input" in {

    // Arrange
    val column1 = "1"

    // Act
    val row = LegalEntityUtils.lookupProfitCentreMapping( ( column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column1.toInt )
    assert( row.get( 1 ) == null )
  }

  "legalEntityMapping" should "generate a Row given 3 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    // Act
    val row = LegalEntityUtils.legalEntityMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )

  }

  "legalEntityMapping" should "generate a Row given 2 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"

    // Act
    val row = LegalEntityUtils.legalEntityMapping( ( column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ) == null )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )

  }
}