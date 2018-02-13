package org.aeb.uk.movielens.processors

import org.aeb.uk.movielens.models.{Movie, Rating}
import org.apache.spark.sql.Dataset

// Todo: make even more generic...
object Transform {

  def toRatingTable( rawFileData: Dataset[String], delimiter: String ): Dataset[Rating] =
    rawFileData.map( line => Rating( line, delimiter ) )

  def toMovieTable( rawFileData: Dataset[String], delimiter: String ): Dataset[Movie] =
    rawFileData.map( line => Movie( line, delimiter ) )

}