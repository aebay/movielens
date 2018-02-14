package org.aeb.uk.movielens.processors

import com.typesafe.config.Config

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by aeb on 28/08/17.
  */
object Job {

  def run( appParams: Config, paths: Map[String, String] )( implicit hiveContext: HiveContext ) {

    val delimiter = appParams.getString( "field.delimiter" )
    val topN = appParams.getInt( "user.param.topnmovies" )

    // ingest data
    val moviesFilePath = paths( "inputPath" ) + "movies.dat"
    val ratingsFilePath = paths( "inputPath" ) + "ratings.dat"
    val moviesData = Ingest.textFile( moviesFilePath )
    val ratingsData = Ingest.textFile( ratingsFilePath )

    // create the base tables and cache to avoid having to rebuild these
    val movies = Transform.toMovieTable( moviesData, delimiter ).toDF.cache
    val ratings = Transform.toRatingTable( ratingsData, delimiter ).toDF.cache

    // create table of movie ratings statistics
    val movieRatings = Generate.movieRatingTable( movies, ratings )

    // create table for top N movies of each user
    val topNMoviesPerUser = Generate.topNMoviesPerUserTable( movies, ratings, topN )

    // persist in parquet format
    val movieRatingsFilePath = paths( "outputPath" ) + "movieRatings.parquet"
    movieRatings.write.parquet( movieRatingsFilePath )
    val topNMoviesPerUserPath = paths( "outputPath" ) + "topNMoviesPerUser.parquet"
    topNMoviesPerUser.write.parquet( topNMoviesPerUserPath )

  }

}
