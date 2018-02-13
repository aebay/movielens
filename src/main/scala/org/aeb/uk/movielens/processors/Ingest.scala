package org.aeb.uk.movielens.processors

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext

object Ingest {

  /**
    * Reads in the text file contents and converts lines to String types.  Returns a Dataset.
    *
    * @param filePath
    * @param hiveContext
    * @return
    */
  def textFile(filePath: String )(implicit hiveContext: HiveContext ): Dataset[String] = {

    import hiveContext.implicits._

    hiveContext.read.text( filePath )
      .as[String]

  }

}
