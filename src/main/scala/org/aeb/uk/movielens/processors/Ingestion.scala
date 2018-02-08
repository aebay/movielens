package org.aeb.uk.movielens.processors

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by aeb on 28/08/17.
  */
object Ingestion {

  /**
    * Reads in file contents and splits on the delimiter.  Returns a Dataset.
    *
    * @param hiveContext
    * @param filePath
    * @param delimiter
    * @return
    */
  def readDataset( hiveContext: HiveContext, filePath: String, delimiter: String ): Dataset[Array[String]] = {

    import hiveContext.implicits._

    hiveContext.read.text( filePath )
      .as[String]
      .map( _.split( delimiter ) )

  }

  def readAsDataset( filePath: String, delimiter: String )( implicit hiveContext: HiveContext ): Dataset[Array[String]] = {

    import hiveContext.implicits._

    hiveContext.read.text( filePath )
      .as[String]
      .map( _.split( delimiter ) )

  }

}
