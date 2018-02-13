package org.aeb.uk.movielens.models

/**
  * Extra apply function provided to create new instances from the text file tokenised lines
  *
  * @param userId
  * @param movieId
  * @param rating
  * @param timestamp
  */
abstract case class Rating private[Rating]( userId: Int, movieId: Int, rating: Int, timestamp: Long ) {

  private def readResolve(): Object =
    Rating.apply( userId, movieId, rating, timestamp )

  def copy( userId: Int = userId, movieId: Int = movieId, rating: Int = rating, timestamp: Long = timestamp ): Rating =
    Rating.apply( userId, movieId, rating, timestamp )

}

object Rating {

  def apply( userId: Int, movieId: Int, rating: Int, timestamp: Long ): Rating =
    new Rating( userId, movieId, rating, timestamp ) {}

  def apply( tokenisedLine: Array[String] ): Rating =
    new Rating( tokenisedLine(0).toInt, tokenisedLine(1).toInt, tokenisedLine(2).toInt, tokenisedLine(3).toLong ) {}

  def apply( line: String, delimiter: String ): Rating =
    Rating.apply( line.split( delimiter ) )

}
