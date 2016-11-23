package com.kainos.enstar.TransformationUnitTesting.test

import com.kainos.enstar.TransformationUnitTesting.AnalysisCodeSplitRiskCodeUtils
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Created by terences on 23/11/2016.
 */
class AnalysisCodeSplitRiskCodeUtilsTest extends FlatSpec with Matchers {

  "lineMapping" should "populate a Row given 3 inputs" in {

    // Arrange
    val column1 = "1"
    val column2 = "2"
    val column3 = "C"

    // Act
    val row = AnalysisCodeSplitRiskCodeUtils.lineMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    row.length should be( 3 )
    row.get( 0 ).getClass should be ( classOf[Integer] )
    row.get( 0 ) should be( column1 toInt )
    row.get( 1 ).getClass should be ( classOf[Integer] )
    row.get( 1 ) should be( column2 toInt )
    row.get( 2 ).getClass should be ( classOf[String] )
    row.get( 2 ) should be( column3 )

  }

  "lineMapping" should "populate a Row given 2 inputs" in {

    // Arrange
    val column1 = "1"
    val column2 = "2"

    // Act
    val row = AnalysisCodeSplitRiskCodeUtils.lineMapping( ( column1 :: column2 :: Nil ).toArray )

    // Assert
    row.length should be( 3 )
    row.get( 0 ).getClass should be ( classOf[Integer] )
    row.get( 0 ) should be( column1 toInt )
    row.get( 1 ).getClass should be ( classOf[Integer] )
    row.get( 1 ) should be( column2 toInt )
    assert( null == row.get( 2 ) )
  }

  "lineMapping" should "throw error when passed less than 2 inputs" in {

    // Arrange
    val column1 = "1"

    // Act Assert
    intercept[ArrayIndexOutOfBoundsException]{
      AnalysisCodeSplitRiskCodeUtils.lineMapping( ( column1 :: Nil ).toArray )
    }
  }

  "lineMapping" should "throw error when first column is not parsable as an int" in {

    // Arrange
    val column1 = "error"
    val column2 = "2"
    val column3 = "C"

    // Act
    a[NumberFormatException] should be thrownBy {
      AnalysisCodeSplitRiskCodeUtils.lineMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )
    }

  }

  "lineMapping" should "throw error when second column is not parsable as an int" in {

    val column1 = "1"
    val column2 = "error"
    val column3 = "C"

    // Act Assert
    a[NumberFormatException] should be thrownBy {
      AnalysisCodeSplitRiskCodeUtils.lineMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )
    }

  }

  "lineriskcodeMapping" should "populate a Row given 4 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"

    // Act
    val row = AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping( ( column1 :: column2 :: column3 :: column4 :: Nil ).toArray )

    // Assert
    row.length should be( 4 )
    row.get( 0 ).getClass should be ( classOf[String] )
    row.get( 0 ) should be( column1 )
    row.get( 1 ).getClass should be ( classOf[String] )
    row.get( 1 ) should be( column2 )
    row.get( 2 ).getClass should be ( classOf[String] )
    row.get( 2 ) should be( column3 )
  }

  "lineriskcodeMapping" should "throw error when passed less than 4 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"

    // Act Assert
    a[ArrayIndexOutOfBoundsException] should be thrownBy {
      AnalysisCodeSplitRiskCodeUtils.lineriskcodeMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )
    }

  }
}
