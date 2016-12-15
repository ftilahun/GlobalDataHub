package com.kainos.enstar.test.TransformationUnitTesting.AnalysisCodeSplitRiskCode

import com.kainos.enstar.TransformationUnitTesting.AnalysisCodeSplitRiskCodeUtils
import org.scalatest.{ FlatSpec, Matchers }

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

  "lineriskcodeMapping" should "populate a Row given 3 inputs" in {

    // Arrange
    val column1 = "1"
    val column2 = "B"
    val column3 = "C"

    // Act
    val row = AnalysisCodeSplitRiskCodeUtils.lineRiskCodeMapping( ( column1 :: column2 :: column3 :: Nil ).toArray )

    // Assert
    row.length should be( 3 )
    row.get( 0 ).getClass should be ( classOf[Integer] )
    row.get( 0 ) should be( column1.toInt )
    row.get( 1 ).getClass should be ( classOf[String] )
    row.get( 1 ) should be( column2 )
    row.get( 2 ).getClass should be ( classOf[String] )
    row.get( 2 ) should be( column3 )
  }

  "lineriskcodeMapping" should "throw error when passed less than 3 inputs" in {

    // Arrange
    val column1 = "1"
    val column2 = "B"

    // Act Assert
    a[ArrayIndexOutOfBoundsException] should be thrownBy {
      AnalysisCodeSplitRiskCodeUtils.lineRiskCodeMapping( ( column1 :: column2 :: Nil ).toArray )
    }

  }

  "lineriskcodeMapping" should "throw error when first column is not a parsable int" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"

    // Act Assert
    a[NumberFormatException] should be thrownBy {
      AnalysisCodeSplitRiskCodeUtils.lineRiskCodeMapping( ( column1 :: column2 :: Nil ).toArray )
    }

  }

  "analysiscodesplitMapping" should "generate a row when passed 8 inputs" in {

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
    val row = AnalysisCodeSplitRiskCodeUtils.analysisCodeSplitMapping( ( column1 :: column2 :: column3 :: column4 ::
      column5 :: column6 :: column7 :: column8 :: Nil ).toArray )

    // Assert
    row.length should be( 8 )
    row.get( 0 ).getClass should be ( classOf[String] )
    row.get( 0 ) should be( column1 )
    row.get( 1 ).getClass should be ( classOf[String] )
    row.get( 1 ) should be( column2 )
    row.get( 2 ).getClass should be ( classOf[String] )
    row.get( 2 ) should be( column3 )
    row.get( 3 ).getClass should be ( classOf[String] )
    row.get( 3 ) should be( column4 )
    row.get( 4 ).getClass should be ( classOf[String] )
    row.get( 4 ) should be( column5 )
    row.get( 5 ).getClass should be ( classOf[String] )
    row.get( 5 ) should be( column6 )
    row.get( 6 ).getClass should be ( classOf[String] )
    row.get( 6 ) should be( column7 )
    row.get( 7 ).getClass should be ( classOf[String] )
    row.get( 7 ) should be( column8 )

  }

  "analysiscodesplitMapping" should "generate a row when passed null inputs" in {

    // Arrange
    val column1 = ""
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"
    val column5 = "E"
    val column6 = "F"
    val column7 = "G"
    val column8 = "H"

    // Act
    val row = AnalysisCodeSplitRiskCodeUtils.analysisCodeSplitMapping( ( column1 :: column2 :: column3 :: column4 ::
      column5 :: column6 :: column7 :: column8 :: Nil ).toArray )

    // Assert
    row.length should be( 8 )
    assert( null == row.get( 0 ) )
    row.get( 1 ).getClass should be ( classOf[String] )
    row.get( 1 ) should be( column2 )
    row.get( 2 ).getClass should be ( classOf[String] )
    row.get( 2 ) should be( column3 )
    row.get( 3 ).getClass should be ( classOf[String] )
    row.get( 3 ) should be( column4 )
    row.get( 4 ).getClass should be ( classOf[String] )
    row.get( 4 ) should be( column5 )
    row.get( 5 ).getClass should be ( classOf[String] )
    row.get( 5 ) should be( column6 )
    row.get( 6 ).getClass should be ( classOf[String] )
    row.get( 6 ) should be( column7 )
    row.get( 7 ).getClass should be ( classOf[String] )
    row.get( 7 ) should be( column8 )
  }

  "analysiscodeplitMapping" should "throw an error when passed less than 7 inputs" in {

    // Arrange
    val column1 = "A"
    val column2 = "B"
    val column3 = "C"
    val column4 = "D"
    val column5 = "E"
    val column6 = "F"

    // Act
    a[ArrayIndexOutOfBoundsException] should be thrownBy {
      AnalysisCodeSplitRiskCodeUtils.analysisCodeSplitMapping( ( column1 :: column2 :: column3 :: column4 :: column5 :: column6 :: Nil ).toArray )
    }
  }

  // TODO Update for added field

}
