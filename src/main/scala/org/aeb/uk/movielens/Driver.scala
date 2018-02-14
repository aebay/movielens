package org.aeb.uk.movielens

import com.typesafe.config.{Config, ConfigFactory}
import org.aeb.uk.movielens.processors.Job
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aeb on 28/08/17.
  */
object Driver extends App {

  /**
    * Generic function for retrieving Java system parameters
    *
    * @param value
    * @return
    */
  def getSystemProperty( value: String ): String = {

    val systemProperty = System.getProperty( value )
    if ( systemProperty == null || systemProperty.isEmpty() )
      System.exit( 1 )

    systemProperty

  }

  /**
    * Retrieve the input and output directory paths
    *
    * @return
    */
  def loadPaths(): Map[String, String] = {

    val fileSeparator = getSystemProperty( "file.separator" )

    val inputPath = getSystemProperty( "inputPath" ) + fileSeparator
    val outputPath = getSystemProperty( "outputPath" ) + fileSeparator

    Map( ("inputPath", inputPath), ("outputPath", outputPath) )

  }

  // load configurations
  val sparkParams: Config = ConfigFactory.load( "spark.properties" )
  val appParams: Config = ConfigFactory.load( "application.properties" )

  // load input and output directory paths
  val paths = loadPaths()

  // create Hive context
  val sparkConf = new SparkConf()
    .setAppName( sparkParams.getString( "app.name" ) )
    .setMaster( sparkParams.getString( "spark.master" ) )

  val sparkContext = new SparkContext( sparkConf )
  implicit val hiveContext = new HiveContext( sparkContext )

  // launch executor
  Job.run( appParams, paths )

  // close context
  sparkContext.stop()

}