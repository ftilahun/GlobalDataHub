package com.kainos.enstar.test.TransformationUnitTesting.Deduction

import com.kainos.enstar.TransformationUnitTesting.DeductionUtils
import org.scalatest.FlatSpec

/**
 * Created by terences on 08/12/2016.
 */
class DeductionUtilsTests extends FlatSpec {

  "lineMapping" should "generate a Row given 6 inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "A"
    val column3 = "B"
    val column4 = "C"
    val column5 = "D"

    val columns = Array( column0, column1, column2, column3, column4, column5 )

    // Act
    val row = DeductionUtils.lineMapping( columns )

    // Assert
    assert( row.size == 6 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )

  }

  "lineMapping" should "throw and error when first column is not parsable as int" in {

    // Arrange
    val column0 = "Error"
    val column1 = "2"
    val column2 = "A"
    val column3 = "B"
    val column4 = "C"
    val column5 = "D"

    val columns = Array( column0, column1, column2, column3, column4, column5 )

    // Act
    intercept[NumberFormatException]{
      DeductionUtils.lineMapping( columns )
    }
  }

  "lineMapping" should "throw and error when second column is not parsable as int" in {

    // Arrange
    val column0 = "1"
    val column1 = "Error"
    val column2 = "A"
    val column3 = "B"
    val column4 = "C"
    val column5 = "D"

    val columns = Array( column0, column1, column2, column3, column4, column5 )

    // Act
    intercept[NumberFormatException]{
      DeductionUtils.lineMapping( columns )
    }
  }

  "layerMapping" should "generate a Row given 2 inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "A"

    val columns = Array( column0, column1 )

    // Act
    val row = DeductionUtils.layerMapping( columns )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "layerMapping" should "throw an error when first column is not parsable as an int" in {

    // Arrange
    val column0 = "Error"
    val column1 = "A"

    val columns = Array( column0, column1 )

    // Act
    intercept[NumberFormatException]{
      DeductionUtils.layerMapping( columns )
    }
  }

  "layerDeductionMapping" should "generate a Row given 5 inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "3"
    val column3 = "B"
    val column4 = "C"

    val columns = Array( column0, column1, column2, column3, column4 )

    // Act
    val row = DeductionUtils.layerDeductionMapping( columns )

    // Assert
    assert( row.size == 5 )
    assert( row.get( 0 ) == column0.toInt )
    assert( row.get( 1 ) == column1.toInt )
    assert( row.get( 2 ) == column2.toInt )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )

  }

  "layerDeductionMapping" should "throw an error when first column is not parsable as an int" in {

    // Arrange
    val column0 = "Error"
    val column1 = "2"
    val column2 = "3"
    val column3 = "B"
    val column4 = "C"

    val columns = Array( column0, column1, column2, column3, column4 )

    // Act
    intercept[NumberFormatException]{
      DeductionUtils.layerDeductionMapping( columns )
    }
  }

  "layerDeductionMapping" should "throw an error when second column is not parsable as an int" in {

    // Arrange
    val column0 = "1"
    val column1 = "Error"
    val column2 = "3"
    val column3 = "B"
    val column4 = "C"

    val columns = Array( column0, column1, column2, column3, column4 )

    // Act
    intercept[NumberFormatException]{
      DeductionUtils.layerDeductionMapping( columns )
    }
  }

  "layerDeductionMapping" should "throw an error when third column is not parsable as an int" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "Error"
    val column3 = "B"
    val column4 = "C"

    val columns = Array( column0, column1, column2, column3, column4 )

    // Act
    intercept[NumberFormatException]{
      DeductionUtils.layerDeductionMapping( columns )
    }
  }

  "deductionMapping" should "generate a Row given 12 inputs" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "A"
    val column3 = "B"
    val column4 = "C"
    val column5 = "D"
    val column6 = "E"
    val column7 = "F"
    val column8 = "G"
    val column9 = "H"
    val column10 = "true"
    val column11 = "false"

    val columns = Array( column0, column1, column2, column3, column4, column5,
      column6, column7, column8, column9, column10, column11 )

    // Act
    val row = DeductionUtils.deductionMapping( columns )

    // Assert
    assert( row.size == 12 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )
    assert( row.get( 3 ).equals( column3 ) )
    assert( row.get( 4 ).equals( column4 ) )
    assert( row.get( 5 ).equals( column5 ) )
    assert( row.get( 6 ).equals( column6 ) )
    assert( row.get( 7 ).equals( column7 ) )
    assert( row.get( 8 ).equals( column8 ) )
    assert( row.get( 9 ).equals( column9 ) )
    assert( row.get( 10 ) == column10.toBoolean )
    assert( row.get( 11 ) == column11.toBoolean )

  }

  "deductionMapping" should "throw an error when 11th column is not parsable as boolean" in {

    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "A"
    val column3 = "B"
    val column4 = "C"
    val column5 = "D"
    val column6 = "E"
    val column7 = "F"
    val column8 = "G"
    val column9 = "H"
    val column10 = "error"
    val column11 = "false"

    val columns = Array( column0, column1, column2, column3, column4, column5,
      column6, column7, column8, column9, column10, column11 )

    // Act
    intercept[IllegalArgumentException] {
      DeductionUtils.deductionMapping( columns )
    }
  }

  "deductionMapping" should "throw an error with 12th column is not parsable as boolean" in {
    // Arrange
    val column0 = "1"
    val column1 = "2"
    val column2 = "A"
    val column3 = "B"
    val column4 = "C"
    val column5 = "D"
    val column6 = "E"
    val column7 = "F"
    val column8 = "G"
    val column9 = "H"
    val column10 = "true"
    val column11 = "error"

    val columns = Array( column0, column1, column2, column3, column4, column5,
      column6, column7, column8, column9, column10, column11 )

    // Act
    intercept[IllegalArgumentException] {
      DeductionUtils.deductionMapping( columns )
    }
  }
}
