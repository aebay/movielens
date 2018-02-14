package org.aeb.uk.movielens.processors

import org.aeb.uk.movielens.models.{Movie, Rating}
import org.apache.spark.sql.{DataFrame, Dataset}

// Todo: make even more generic...
object Transform {

  /**
    * Converts the raw file data into a Dataset of Rating classes.
    *
    * @param rawFileData
    * @param delimiter
    * @return
    */
  def toRatingTable( rawFileData: Dataset[String], delimiter: String ): Dataset[Rating] =
    rawFileData.map( line => Rating( line, delimiter ) )

  /**
    * Converts the raw file data into a Dataset of Movie classes.
    *
    * @param rawFileData
    * @param delimiter
    * @return
    */
  def toMovieTable( rawFileData: Dataset[String], delimiter: String ): Dataset[Movie] =
    rawFileData.map( line => Movie( line, delimiter ) )

}