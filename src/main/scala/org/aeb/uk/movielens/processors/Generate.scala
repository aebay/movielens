package org.aeb.uk.movielens.processors

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, desc, min, max, row_number}
import org.apache.spark.sql.hive.HiveContext

object Generate {

  /**
    * Returns a DataFrame including the minimum, maximum and average user ratings for each film.
    *
    * Output is sorted in ascending movieId.
    *
    * @param movies
    * @param ratings
    * @return
    */
  def movieRatingTable( movies: DataFrame, ratings: DataFrame ): DataFrame = {

    // discard unused columns
    val simpleRatings = ratings
      .drop( "userId" )
      .drop( "timestamp" )

    // calculate the ratings statistics
    movies.join( simpleRatings, Seq("movieId") )                                                        // join with the ratings table on movie ID
      .agg( min("rating") as "minRating", max("rating") as "maxRating", avg("rating") as "avgRating" )  // group on unnamed columns and calculate statistics
      .orderBy( asc("movieId") )                                                                        // sort by movie ID in ascending order

    // register the staging table for the SQL query
    //movieRatingsStaging.registerTempTable( "movieRatingsStaging" )


    // calculate the aggregations
    /*hiveContext.sql( "SELECT movieId, title, genres, " +
      "MIN(rating) AS minRating, MAX(rating) AS maxRating, AVG(rating) AS avgRating " +
      "FROM movieRatingsStaging GROUP BY movieId, title, genres ORDER BY movieId ASC" )*/

  }

  /**
    * <p>
    *   Returns a DataFrame containing the top N films per user as ranked by them.  In the event of
    *   a tie in the ranking, the top N films are ranked by movie ID.
    *
    *   Output is sorted by ascending user ID and movie ID.  The movie name is included in the data
    *   to make it more readable.
    * </p>
    *
    * @param movies
    * @param ratings
    * @param topN
    * @param hiveContext
    * @return
    */
  def topNMoviesPerUserTable( movies: DataFrame, ratings: DataFrame, topN: Int )( implicit hiveContext: HiveContext): DataFrame = {

    import hiveContext.implicits._ // required for use of "where" method

    // define the analysis window
    val userWindow = Window.partitionBy( "userId" ).orderBy( desc("rating") ) // partition the table on userId and order each partition on rating in descending order

    // discard unused columns
    val simpleRatings = ratings
      .drop( "timestamp" )
    val simpleMovies = movies
      .drop( "genres" )

    // calculate the top N rated films per user
    val topNUserRatings = simpleRatings
      .orderBy( asc("movieId") )                                // ???
      .withColumn( "rank", row_number.over( userWindow ) )      // apply ranking to the partition
      .where( $"rank" <= topN )                                 // remove rows where the rank is less than or equal to the maximum one of interest
      .drop( "rank" ) // remove unnecessary ranking column      // remove the ranking column as it's no longer needed
      .orderBy( asc( "userId" ), asc( "movieId" ) )             // order by user ID, then movieId in ascending order

    // add movie descriptions
    topNUserRatings.join( simpleMovies, Seq( "movieId" ) )

  }

}