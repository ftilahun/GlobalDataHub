package com.kainos.enstar.transformation

import org.apache.spark.sql.types._
import org.scalatest.{ Matchers, WordSpec }

import scala.math.BigDecimal

/**
 * Created by neilri on 16/12/2016.
 */
class SchemaSpec extends WordSpec with Matchers {

  "A Schema" when {
    "creating from headers" should {
      "evaluate long, int, string and decimal headers" in {
        val headers = Array( "col0[long]", "col1[int]", "col2[string]", "col3[decimal(10:2)]" )
        val struct = Schema.structFromHeaders( headers )

        struct.length should be ( 4 )
        struct( 0 ) should be ( StructField( "col0", LongType, false ) )
        struct( 1 ) should be ( StructField( "col1", IntegerType, false ) )
        struct( 2 ) should be ( StructField( "col2", StringType, false ) )
        struct( 3 ) should be ( StructField( "col3", DecimalType( 10, 2 ), false ) )
      }

      "support nullable columns" in {
        val headers = Array( "col0[int?]" )
        val struct = Schema.structFromHeaders( headers )

        struct.length should be ( 1 )
        struct( 0 ) should be ( StructField( "col0", IntegerType, true ) )
      }

      "fail if incorrect type specified" in {
        val headers = Array( "col0[varchar]" )
        an[IllegalArgumentException] should be thrownBy {
          val struct = Schema.structFromHeaders( headers )
        }
      }
    }

    "converting rows according to schema" should {
      "convert long, int, string and decimals fields" in {
        val headers = Array( "col0[long]", "col1[int]", "col2[string]", "col3[decimal(10:2)]" )
        val schema = Schema( headers )

        val data = Array( "9223372036854775807", "2147483647", "A string", "99999999.99" )

        val x = schema.stringFieldsToAny( data )

        x( 0 ) shouldBe a[java.lang.Long]
        x( 0 ) should be ( 9223372036854775807l )

        x( 1 ) shouldBe a[java.lang.Integer]
        x( 1 ) should be ( 2147483647 )

        x( 2 ) shouldBe a[String]
        x( 2 ) should be ( "A string" )

        x( 3 ) shouldBe a[scala.math.BigDecimal]
        x( 3 ) should be ( BigDecimal( "99999999.99" ) )
      }

      "support nullable columns" in {
        val headers = Array( "col0[long?]", "col1[int?]", "col2[string?]", "col3[decimal(10:2)?]" )
        val schema = Schema( headers )

        val data = Array( "[NULL]", "[NULL]", "[NULL]", "[NULL]" )

        val x = schema.stringFieldsToAny( data )

        x( 0 ) should be ( null : java.lang.Long )
        x( 1 ) should be ( null : java.lang.Integer )
        x( 2 ) should be ( null : String )
        x( 3 ) should be ( null : scala.math.BigDecimal )
      }

      "fail if null values are stored in not-null fields" in {
        val headers = Array( "name[string]" )
        val schema = Schema( headers )

        val data = Array( "[NULL]" )

        an[IllegalArgumentException] should be thrownBy {
          val x = schema.stringFieldsToAny( data )
        }
      }

      "allow null values to be ignored in not-null fields" in {
        val headers = Array( "name[string]" )
        val ignoreNullable = true
        val schema = Schema( headers, ignoreNullable )

        val data = Array( "[NULL]" )
        val x = schema.stringFieldsToAny( data )

        x( 0 ) should be ( null : String )
      }

      "fail if incorrect number of fields passed for schema" in {
        val headers = Array( "col0[string]", "col1[string]" )
        val schema = Schema( headers )

        val data1 = Array( "One column" )

        an[IllegalArgumentException] should be thrownBy {
          val x = schema.stringFieldsToAny( data1 )
        }

        val data2 = Array( "col1", "col2", "col3" )
        an[IllegalArgumentException] should be thrownBy {
          val x = schema.stringFieldsToAny( data2 )
        }
      }
    }
  }
}
