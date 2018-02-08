package org.aeb.uk.spark

import org.apache.spark.sql.hive.HiveContext

trait SparkHiveSuite extends SparkSuite {

  implicit var hiveContext: HiveContext = _

  override def beforeAll(): Unit = {

    super.beforeAll()

    hiveContext = new HiveContext( sparkContext )

  }

  override def afterAll(): Unit = {

    super.afterAll()

  }

}
