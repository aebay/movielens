package org.aeb.uk.movielens.models

/**
  * Used for GenerateIT test to create dummy data.
  *
  * @param movieId
  * @param title
  * @param genres
  * @param rating
  */
case class MovieRating( movieId: Int, title: String, genres: String, rating: Int )
