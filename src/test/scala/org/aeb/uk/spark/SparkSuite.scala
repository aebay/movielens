package org.aeb.uk.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait SparkSuite extends FlatSpec with BeforeAndAfterAll {

  implicit var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {

    super.beforeAll()

    val sparkConf = new SparkConf()
      .setMaster( "local[*]" )
      .setAppName( this.getClass.getSimpleName )

    sparkContext = new SparkContext( sparkConf )

  }

  override def afterAll(): Unit = {

    if ( sparkContext != null ) {
      sparkContext.stop()
      sparkContext = null
    }

    super.afterAll()

  }

}