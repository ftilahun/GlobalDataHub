package com.kainos.enstar.transformation

import java.net.URL

import scala.io.Source

/**
 * Created by neilri on 16/12/2016.
 */
class CsvReader( url : URL ) {

  // load all the file into memory so we can be sure we're reading the first row for the header
  private val source = Source.fromURL( url )
  private val lines = try source.getLines().toList finally source.close()

  val headers : Array[String] = lineToFields( lines.head )

  val body : List[Array[String]] = lines.tail map lineToFields

  // basic implementation just splits on comma
  // can be replaced with more advanced impl if we need to include quotes, escaping, etc
  private def lineToFields( rawRow : String ) : Array[String] = rawRow.split( "," )
}

object CsvReader {
  def apply( url : URL ) : CsvReader = new CsvReader( url )
}