package com.kainos.enstar.test.TransformationUnitTesting.GeographyType
import com.kainos.enstar.TransformationUnitTesting.GeographyTypeUtils
import org.scalatest.FlatSpec

/**
 * Created by adamf on 29/11/2016.
 */
class GeographyTypeUtilsTests extends FlatSpec {
  // There is no corresponding table for this mapping so there is no entry data, thus a null row
  "lookupGeographyTypeMapping" should "generate a null Row" in {
    val row = GeographyTypeUtils.lookupGeographyTypeMapping( null )
    assert( row.get( 0 ) == null )
  }

  "geographyMapping" should "generate a Row with 3 strings" in {
    val row = GeographyTypeUtils.geographyMapping( Array ( "1", "2", "3" ) )
    assert( row.get( 0 ) == "1" )
    assert( row.get( 1 ) == "2" )
    assert( row.get( 2 ) == "3" )
  }
}
