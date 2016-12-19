package com.kainos.enstar.transformation

import org.scalatest.{ WordSpec, Matchers }

/**
 * Created by neilri on 19/12/2016.
 */
class CsvReaderSpec extends WordSpec with Matchers {

  val testInputGood = getClass.getResource( "/com/kainos/enstar/transformation/test_input_good.csv" )
  val testInputBadNulls = getClass.getResource( "/com/kainos/enstar/transformation/test_input_bad_nulls.csv" )
  val doesntExist = getClass.getResource( "/com/kainos/enstar/transformation/doesnt_exist.csv" )

  "A CsvReader" when {
    "being created" should {
      "work with a resource that exists" in {
        val csvReader = CsvReader( testInputGood )

      }
      "throw a NullPointerException for resources that don't exist" in {
        a[NullPointerException] should be thrownBy {
          val csvReader = CsvReader( doesntExist )
        }
      }
    }

    "reading the file" should {
      "use the first line as the header" in {
        val csvReader = CsvReader( testInputGood )
        // name[string],count[int],description[string?]
        csvReader.headers.length should be ( 3 ) // 3 columns
        csvReader.headers( 0 ) should be ( "name[string]" )
        csvReader.headers( 1 ) should be ( "count[int]" )
        csvReader.headers( 2 ) should be ( "description[string?]" )
      }

      "use the rest of the lines as the body" in {
        val csvReader = CsvReader( testInputGood )
        // file is 4 lines long so subtracting first line gives 3 rows
        csvReader.body.length should be ( 3 )
      }
    }
  }
}
