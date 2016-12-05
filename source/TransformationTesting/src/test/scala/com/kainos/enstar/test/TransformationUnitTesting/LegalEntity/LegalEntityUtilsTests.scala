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

  "legalEntityMapping" should "generate a Row given 7 inputs" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"
    val column3 = "D"
    val column4 = "E"
    val column5 = "false"
    val column6 = "F"

    val columns = Array( column0, column1, column2, column3, column4, column5, column6 )

    // Act
    val row = LegalEntityUtils.legalEntityMapping( columns )

    // Assert
    assert( row.size == 7 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( false == row.get( 5 ) )
    assert( row.get( 6 ).equals( column6 ) )

  }

  "legalEntityMapping" should "generate a Row given empty strings for column 3 and 4" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"
    val column3 = ""
    val column4 = ""
    val column5 = "false"
    val column6 = "E"

    val columns = Array( column0, column1, column2, column3, column4, column5, column6 )

    // Act
    val row = LegalEntityUtils.legalEntityMapping( columns )

    // Assert
    assert( row.size == 7 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )
    assert( false == row.get( 5 ) )
    assert( row.get( 6 ).equals( column6 ) )

  }

  "legalEntityMapping" should "generate a Row given empty strings for column 2, 3 and 4" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = ""
    val column3 = ""
    val column4 = ""
    val column5 = "false"
    val column6 = "C"

    val columns = Array( column0, column1, column2, column3, column4, column5, column6 )

    // Act
    val row = LegalEntityUtils.legalEntityMapping( columns )

    // Assert
    assert( row.size == 7 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ) == null )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )
    assert( false == row.get( 5 ) )
    assert( row.get( 6 ).equals( column6 ) )

  }

}
