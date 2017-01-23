package com.kainos.enstar.transformation.runner

object TransformationArgumentParser {

  val parser = new scopt.OptionParser[TransformationArguments]( "TransformationJob" ) {
    head( "TransformationJob", "0.3" )

    opt[String]( "sourceDB" ).required().action(
      ( d, t ) => t.copy( sourceDB = d )
    ).text( "Source data database name E.g. ndex" )

    opt[String]( "sourceType" ).required().action(
      ( s, t ) => t.copy( sourceSystem = s.toLowerCase )
    ).text( "Source system E.g. ndex" )

    opt[String]( "outputPath" ).required().action(
      ( o, t ) => t.copy( outputPath = o )
    ).text( "HDFS output path for transformed data" )
  }

  def parseArguments( args : Array[String] ) = parser.parse( args, TransformationArguments() )
}
