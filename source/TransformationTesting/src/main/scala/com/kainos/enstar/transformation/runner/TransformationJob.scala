package com.kainos.enstar.transformation.runner

import org.apache.spark.sql.DataFrame
import com.databricks.spark.avro._
import collection.JavaConversions._
import org.reflections.Reflections
import org.reflections.scanners.ResourcesScanner
import org.reflections.util.{ ClasspathHelper, ConfigurationBuilder }
import java.util.regex.Pattern

object TransformationJob extends App {

  val transformationArguments = TransformationArgumentParser.parseArguments( args ) match {
    case Some( transformArguments ) => transformArguments
    case None                       => throw new IllegalArgumentException
  }

  val transformationContext = new TransformationContext()
  transformationContext.setSourceDB( transformationArguments.sourceDB )

  val transformationFileNames = getTransformationFileNames( transformationArguments.sourceSystem )

  val tableNamesAndDataFrames = transformationFileNames.map(
    file => ( tableNameFromFileName( file ), transformationContext.runTransformation( file ) )
  ).toMap

  writeDataFramesAsAvro( tableNamesAndDataFrames, transformationArguments.outputPath )

  private def getTransformationFileNames( sourceSystem : String ) : List[String] = {
    val packageName = "Transformation"
    val transformationFilePattern = packageName + "/" + sourceSystem + "/.+\\.hql"

    val config = new ConfigurationBuilder().setScanners(
      new ResourcesScanner ).setUrls( ClasspathHelper.forPackage( packageName )
      )

    val allHqlResources = new Reflections( config ).getResources( Pattern.compile( """.+\.hql""" ) )
    allHqlResources.filter( _.matches( transformationFilePattern ) ).toList
  }

  private def tableNameFromFileName( fileName : String ) : String = {
    val fileNamePattern = "Transformation/[a-zA-Z0-9]+/([a-zA-Z0-9]+).hql".r

    fileName match {
      case fileNamePattern( tableName ) => tableName.toLowerCase
    }
  }

  private def writeDataFramesAsAvro( transformDataFrames : Map[String, DataFrame], outputPath : String ) : Unit = {
    for ( ( tableName, dataFrame ) <- transformDataFrames )
      dataFrame.repartition( 1 ).write.avro( s"$outputPath/$tableName" )
  }
}
