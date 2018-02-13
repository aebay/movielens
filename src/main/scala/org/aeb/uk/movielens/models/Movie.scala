package org.aeb.uk.movielens.models

/**
  * Extra apply function provided to create new instances from the text file tokenised lines
  *
  * @param movieId
  * @param title
  * @param genres
  */
abstract case class Movie private[Movie]( movieId: Int, title: String, genres: String ) {

  private def readResolve(): Object =
    Movie.apply( movieId, title, genres )

  def copy( movieId: Int = movieId, title: String = title, genres: String = genres ): Movie =
    Movie.apply( movieId, title, genres )

}

object Movie {

  def apply( movieId: Int, title: String, genres: String ): Movie =
    new Movie( movieId, title, genres ) {}

  def apply( tokenisedLine: Array[String] ): Movie =
    new Movie( tokenisedLine(0).toInt, tokenisedLine(1), tokenisedLine(2) ) {}

  def apply( line: String, delimiter: String ): Movie =
    Movie.apply( line.split( delimiter ) )

}