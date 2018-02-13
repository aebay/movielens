package org.aeb.uk.movielens.processors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, desc, row_number}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by aeb on 29/08/17.
  */
object Generate {

  /**
    * Returns a Dataframe including the minimum, maximum and average user ratings for each film.
    *
    * @param hiveContext
    * @param movies
    * @param ratings
    * @return
    */
  def movieRatingsTable(hiveContext: HiveContext, movies: DataFrame, ratings: DataFrame ): DataFrame = {

    // simplify the ratings DataFrame
    val simpleRatings = ratings.drop( "userId" )
      .drop( "timestamp" )

    // create the staging table
    val movieRatingsStaging = movies.join( ratings, Seq("movieId") )

    // register the staging table for the SQL query
    movieRatingsStaging.registerTempTable( "movieRatingsStaging" )

    // calculate the aggregations
    hiveContext.sql( "SELECT movieId, title, genres, " +
      "MIN(rating) AS minRating, MAX(rating) AS maxRating, AVG(rating) AS avgRating " +
      "FROM movieRatingsStaging GROUP BY movieId, title, genres ORDER BY movieId ASC" )

  }

  /**
    * <p>
    *   Returns a DataFrame containing the top N films per user as ranked by them.  In the event of
    *   a tie in the ranking, the top N films are ranked by movie ID.
    *
    *   Extra conditioning is applied to the output, for example sorting by user ID and movie ID
    *   and including the movie name in the data to make it more readable.
    * </p>
    *
    * @param hiveContext
    * @param movies
    * @param ratings
    * @param topN
    * @return
    */
  def topNMoviesPerUserTable(hiveContext: HiveContext, movies: DataFrame, ratings: DataFrame, topN: Int ): DataFrame = {

    import hiveContext.implicits._

    // define the analysis window
    val userWindow = Window.partitionBy( "userId" ).orderBy( desc("rating") )

    // calculate the top N rated films per user
    val topNUserRatings = ratings.drop( "timestamp" )
      .orderBy( asc("movieId") )
      .withColumn( "rank", row_number.over( userWindow ) )
      .where( $"rank" <= topN )
      .drop( "rank" )
      .orderBy( asc( "userId" ), asc( "movieId" ) )

    // add movie descriptions
    topNUserRatings.join( movies.drop( "genres" ), Seq( "movieId" ) )

  }

}