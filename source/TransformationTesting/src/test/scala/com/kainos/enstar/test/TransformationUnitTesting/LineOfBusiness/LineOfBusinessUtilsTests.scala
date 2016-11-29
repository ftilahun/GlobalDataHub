package com.kainos.enstar.test.TransformationUnitTesting.LineOfBusiness

import com.kainos.enstar.TransformationUnitTesting.LineOfBusinessUtils
import org.scalatest.FlatSpec

/**
 * Created by sionam on 24/11/2016.
 */
class LineOfBusinessUtilsTests extends FlatSpec {

  "lookupBlockMapping" should "generate a Row" in {

    // Arrange
    val column1 = "1"
    val column2 = "2"

    // Act
    val row = LineOfBusinessUtils.lookupBlockMapping( ( column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )

  }

  "underwritingBlockMapping" should "generate a Row" in {

    // Arrange
    val column1 = "1"
    val column2 = "2"
    val column3 = "3"

    // Act
    val row = LineOfBusinessUtils.underwritingBlockMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ) == column1 )
    assert( row.get( 1 ) == column2 )
    assert( row.get( 2 ) == column3 )

  }

  "lineOfBusinessMapping" should "generate a Row given 3 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    // Act
    val row = LineOfBusinessUtils.lineOfBusinessMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ) == null )
    assert( row.get( 4 ) == null )

  }

  "lineOfBusinessMapping" should "generate a Row given 4 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"

    // Act
    val row = LineOfBusinessUtils.lineOfBusinessMapping( ( column1 :: column2 :: column3 :: column4 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ).equals( column4 ) )
    assert( row.get( 4 ) == null )

  }

  "lineOfBusinessMapping" should "generate a Row given 5 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"
    val column5 = "E"

    // Act
    val row = LineOfBusinessUtils.lineOfBusinessMapping( ( column1 :: column2 :: column3 :: column4 :: column5 :: Nil ).toArray )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ).equals( column1 ) )
    assert( row.get( 1 ).equals( column2 ) )
    assert( row.get( 2 ).equals( column3 ) )
    assert( row.get( 3 ).equals( column4 ) )
    assert( row.get( 4 ).equals( column5 ) )

  }
}
