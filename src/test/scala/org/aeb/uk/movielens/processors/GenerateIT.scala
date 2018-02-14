package org.aeb.uk.movielens.processors

import org.aeb.uk.movielens.models.{Movie, MovieRating, Rating}
import org.aeb.uk.spark.SparkHiveSuite
import org.apache.spark.sql.DataFrame

class GenerateIT extends SparkHiveSuite {

  // Todo: seems awkward, must be a better way of doing this...
  private lazy val hiveContextImplicit = hiveContext

  behavior of "Generate module"

  it should "convert a Dataset of Movie strings to a DataFrame of Movie classes" in {

    val movieId = 1
    val title = "Deadpool"
    val genres = "action,comedy"

    val inputRow = s"$movieId.toInt:$title:$genres"
    val input = hiveContext.createDataset(
      sparkContext.parallelize( Array(inputRow, inputRow, inputRow) ) )

    val outputRow = Movie( movieId, title, genres )
    val output = sparkContext.parallelize( Array(outputRow, outputRow, outputRow) )

    val expectedResult = hiveContext.createDataset( output )
    val actualResult = Transform.toMovieTable( input, ":" )

    assert( expectedResult.collect.deep === actualResult.collect.deep )

  }

  it should "convert a Dataset of Rating strings to a Dataset of Rating classes" in {

    val userId = 1
    val movieId = 2
    val rating = 3
    val timestamp = 1234567890L

    val inputRow = s"$userId:$movieId:$rating:$timestamp"
    val input = hiveContext.createDataset(
      sparkContext.parallelize( Array(inputRow, inputRow, inputRow) ) )

    val outputRow = Rating( userId, movieId, rating, timestamp )
    val output = sparkContext.parallelize( Array(outputRow, outputRow, outputRow) )

    val expectedResult = hiveContext.createDataset( output )
    val actualResult = Transform.toRatingTable( input, ":" )

    assert( expectedResult.collect.deep === actualResult.collect.deep )

  }


  it should "generate a DataFrame containing summary statistics of 3 user ratings for 2 movies" in {

    // data for 2 movies from 3 users
    val movies = Array( Movie( 1, "Deadpool", "action,comedy" ),
                        Movie( 2, "Silence of the Lambs", "thriller,crime,horror" ) )
    val ratings = Array( Rating( 1, 1, 1, 1234567890L ),
                         Rating( 1, 2, 5, 1234567890L ),
                         Rating( 2, 1, 3, 1234567890L ),
                         Rating( 2, 2, 3, 1234567890L ),
                         Rating( 3, 1, 4, 1234567890L ),
                         Rating( 3, 2, 2, 1234567890L ) )

    // create input data
    val moviesDataFrame = hiveContext.createDataset(
      sparkContext.parallelize( movies ) ).toDF
    val ratingsDataFrame = hiveContext.createDataset(
      sparkContext.parallelize( ratings ) ).toDF

    // verify results
    val expectedResults = movieRatingOutput( movies, ratings )
    val actualResults = Generate.movieRatingTable( moviesDataFrame, ratingsDataFrame )

    assert( expectedResults.collect.deep === actualResults.collect.deep )

  }

  it should "generate a DataFrame containing the top 3 movies for 2 users as ranked by the users" is pending

  /**
    * Builds a DataFrame table of MovieRatings classes.
    *
    * @param movies
    * @param ratings
    * @return
    */
  private def movieRatingOutput( movies: Array[Movie], ratings: Array[ Rating ]): DataFrame = {

    // simplify rating data to just rating score array for each film
    val ratingsByMovie = ratings
      .map { rating => ( rating.movieId, rating.rating ) }
      .groupBy { case(k, v) => k }
      .values
      .map { array => array.map( _._2 ) }

    // create output DataFrame
    (movies, ratingsByMovie).zipped.flatMap { (movie, ratings) =>
      ratings.map { rating =>
        MovieRating(movie.movieId, movie.title, movie.genres, rating)
      }
    }

  }

}