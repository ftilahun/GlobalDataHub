package com.kainos.enstar.globaldatahub.cdcloader.properties

import com.kainos.enstar.globaldatahub.properties.{ CommandLinePropertyParser, GDHProperties }

class CDCProperties extends GDHProperties with CommandLinePropertyParser {


  val propertyMap = new java.util.HashMap[String,Any]

  override def getBooleanProperty( s : String ) : java.lang.Boolean = ???

  override def checkPropertiesSet() : Unit = ???

  override def getStringProperty( name : String ) : String = ???

  override def getArrayProperty( name : String ) : Array[String] = ???

  override def parseProperties( propertyArray : Array[String] ) : Unit = {
      val parser = new scopt.OptionParser("scopt") {
        opt(
          "cm",
          "spark.cdcloader.columns.attunity.name.changemask",
        "The name of the attunity change mask column",
          (s : String) => {
            propertyMap.put("spark.cdcloader.columns.attunity.name.changemask",s)
          })
        opt(
          "con",
          "spark.cdcloader.columns.attunity.name.changeoperation",
          "The name of the change operation column",
          (s : String) => {
            propertyMap.put("spark.cdcloader.columns.attunity.name.changeoperation",s)
          })
        opt(
          "csn",
          "spark.cdcloader.columns.attunity.name.changesequence",
          "The name of the attunity change sequence column",
          (s : String) => {
            propertyMap.put("spark.cdcloader.columns.attunity.name.changesequence",s)
          })
        opt(
          "cov",
          "spark.cdcloader.columns.attunity.value.changeoperation",
          "The value of the delete operation in the attunity change operation field ",
          (s : String) => {
            propertyMap.put("spark.cdcloader.columns.attunity.value.changeoperation",s)
          })
        opt(
          "d",
          "spark.cdcloader.columns.metadata.name.isdeleted",
          "The name to give the isDeleted attribute in the outputted avro",
          (s : String) => {
            propertyMap.put("spark.cdcloader.columns.metadata.name.isdeleted",s)
          })
        opt(
          "cst",
          "spark.cdcloader.control.attunity.changetablesuffix",
          "the suffix given to attunity change tables (e.g. __ct)",
          (s : String) => {
            propertyMap.put("spark.cdcloader.control.attunity.changetablesuffix",s)
          })
        opt(
          "ltsn",
          "spark.cdcloader.columns.metadata.name.loadtimestamp",
          "The name to give the load timestamp attribute in the outputted avro",
          (s : String) => {
            propertyMap.put("spark.cdcloader.columns.metadata.name.loadtimestamp",s)
          })
        opt(
          "ats",
          "spark.cdcloader.format.timestamp.attunity",
          "The timestamp format for the attunity change sequence",
          (s : String) => {
            propertyMap.put("spark.cdcloader.format.timestamp.attunity",s)
          })
        opt(
          "hts",
          "spark.cdcloader.format.timestamp.hive",
          "The timestamp format to use for hive",
          (s : String) => {
            propertyMap.put("spark.cdcloader.format.timestamp.hive",s)
          })
        opt(
          "tbls",
          "spark.cdcloader.input.tablenames",
          "A comma separated list of tables to process",
          (s : String) => {
            propertyMap.put("spark.cdcloader.input.tablenames",s)
          })
        opt(
          "i",
          "spark.cdcloader.paths.data.basedir",
          "The input base directory (e.g. /etl/cdc/attunity/ndex/policy/)",
          (s : String) => {
            propertyMap.put("spark.cdcloader.paths.data.basedir",s)
          })
        opt(
          "ic",
          "spark.cdcloader.paths.data.control",
          "The path to the control table",
          (s : String) => {
            propertyMap.put("spark.cdcloader.paths.data.control",s)
          })
        opt(
          "o",
          "spark.cdcloader.paths.data.output",
          "The path to the output directory (e.g. /etl/cdc/cdcloader/ndex/policy/processing/)",
          (s : String) => {
            propertyMap.put("spark.cdcloader.paths.data.output",s)
          })
        opt(
          "sql",
          "spark.cdcloader.paths.sql.basedir",
          "The base directory for sql queries (e.g. /metadata/cdcloader/hive/queries/ndex/",
          (s : String) => {
            propertyMap.put("spark.cdcloader.paths.sql.basedir",s)
          })
        opt(
          "csql",
          "spark.cdcloader.paths.sql.control",
          "The path to the control table sql (e.g. /metadata/cdcloader/hive/queries/control/)",
          (s : String) => {
            propertyMap.put("spark.cdcloader.paths.sql.control",s)
          })
        opt(
          "c",
          "spark.cdcloader.tables.control.name",
          "The name of the control table",
          (s : String) => {
            propertyMap.put("spark.cdcloader.tables.control.name",s)
          })
        booleanOpt(
          "cme",
          "spark.cdcloader.control.changemask.enabled",
          "Should the change mask be enabled?",
          (b: Boolean) => {
            propertyMap.put("spark.cdcloader.control.changemask.enabled",b)
          }
        )
      }
    
      "spark.cdcloader.columns.attunity.name.tablename"
      "spark.cdcloader.control.columnpositions"
    }
}
