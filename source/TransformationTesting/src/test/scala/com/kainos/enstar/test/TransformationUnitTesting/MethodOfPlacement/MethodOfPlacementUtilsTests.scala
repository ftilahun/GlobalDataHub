package com.kainos.enstar.test.TransformationUnitTesting.MethodOfPlacement

import com.kainos.enstar.TransformationUnitTesting.MethodOfPlacementUtils
import org.scalatest.FlatSpec

/**
  * Created by caoimheb on 08/12/2016.
  */
class MethodOfPlacementUtilsTests extends FlatSpec {

  "lookupBusinessTypeMapping" should "generate a Row given 2 inputs" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"

    // Act
    val row = MethodOfPlacementUtils.lookupBusinessTypeMapping( ( column0 :: column1 :: Nil ).toArray )

    // Assert
    assert( row.size == 2 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )

  }

  "methodOfPlacementMapping" should "generate a Row given 3 inputs" in {

    // Arrange
    val column0 = "A"
    val column1 = "B"
    val column2 = "C"

    // Act
    val row = MethodOfPlacementUtils.methodOfPlacementMapping( ( column0 :: column1 :: column2 :: Nil ).toArray )

    // Assert
    assert( row.size == 3 )
    assert( row.get( 0 ).equals( column0 ) )
    assert( row.get( 1 ).equals( column1 ) )
    assert( row.get( 2 ).equals( column2 ) )

  }

}
