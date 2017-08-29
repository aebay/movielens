package org.aeb.uk.movielens.processors

import com.typesafe.config.Config

import org.aeb.uk.movielens.models.{Movie, Rating}
import org.aeb.uk.movielens.processors.BusinessLogic._
import org.aeb.uk.movielens.processors.Ingestion._

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by aeb on 28/08/17.
  */
object Executor {

  def run( hiveContext: HiveContext, appParams: Config, paths: Map[String, String] ) {

    import hiveContext.implicits._

    val delimiter = appParams.getString( "field.delimiter" )
    val topN = appParams.getInt( "user.param.topnmovies" )

    // ingest data
    val moviesFilePath = paths( "inputPath" ) + "movies.dat"
    val movies = readDataset( hiveContext, moviesFilePath, delimiter )
      .map( line => Movie( line(0).toInt, line(1), line(2) ) )
      .toDF.cache

    val ratingsFilePath = paths( "inputPath" ) + "ratings.dat"
    val ratings = readDataset( hiveContext, ratingsFilePath, delimiter )
      .map( line => Rating( line(0).toInt, line(1).toInt, line(2).toInt, line(3).toLong ) )
      .toDF.cache

    // create table of movie ratings statistics
    val movieRatings = processMovieRatings( hiveContext, movies, ratings )

    // create table for top N movies of each user
    val topNMoviesPerUser = processTopNMoviesPerUser( hiveContext, movies, ratings, topN )

    // persist in parquet format
    val movieRatingsFilePath = paths( "outputPath" ) + "movieRatings.parquet"
    movieRatings.write.parquet( movieRatingsFilePath )
    val topNMoviesPerUserPath = paths( "outputPath" ) + "topNMoviesPerUser.parquet"
    topNMoviesPerUser.write.parquet( topNMoviesPerUserPath )

  }

}
