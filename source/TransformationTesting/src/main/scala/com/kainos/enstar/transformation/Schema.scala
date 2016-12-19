package com.kainos.enstar.transformation

import org.apache.spark.sql.types._

/**
 * Created by neilri on 16/12/2016.
 */
class Schema( headers : Array[String] ) {
  import Schema._

  val structType : StructType = structFromHeaders( headers )

  def stringFieldsToAny( fields : Array[String] ) : Array[Any] = {
    if (fields.length != structType.length) throw new IllegalArgumentException( s"Incorrect number of fields in row. Found ${fields.length}, expected ${structType.length}" )
    val fieldsWithType = fields zip structType.toList
    fieldsWithType.map {
      case ( field, fieldType ) =>
        if ( field == NULL ) {
          null // yuck
        } else {
          fieldType.dataType match {
            case StringType      => field
            case LongType        => field.toLong
            case IntegerType     => field.toInt
            case d : DecimalType => BigDecimal( field )
          }
        }
    }
  }
}

object Schema {
  val colDef = """([a-zA-Z]\w*)\[([a-zA-Z]+)(?:\((\d+)\:(\d+)\))?(\?)?\]""".r

  val NULL = "[NULL]"

  def apply( headers : Array[String] ) : Schema = new Schema( headers )

  private[transformation] def structFromHeaders( headers : Array[String] ) : StructType = {
    try {
      val headerFields = headers.map {
        case colDef(name, "string", _, _, nullable) => new StructField(name, StringType, nullable != null)
        case colDef(name, "long", _, _, nullable) => new StructField(name, LongType, nullable != null)
        case colDef(name, "int", _, _, nullable) => new StructField(name, IntegerType, nullable != null)
        case colDef(name, "decimal", precision, scale, nullable) => new StructField(name, DecimalType(precision.toInt, scale.toInt), nullable != null)
      }
      new StructType(headerFields)
    } catch {
      case me: MatchError => throw new IllegalArgumentException(s"Unable to parse column header ${me.getMessage()}")
    }
  }
}