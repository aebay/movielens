package org.aeb.uk.movielens.processors

import com.typesafe.config.Config

import org.aeb.uk.movielens.models.{Movie, Rating}
import org.aeb.uk.movielens.processors.Generate._

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by aeb on 28/08/17.
  */
object Executor {

  def run( appParams: Config, paths: Map[String, String] )( implicit hiveContext: HiveContext ) {

    import hiveContext.implicits._

    val delimiter = appParams.getString( "field.delimiter" )
    val topN = appParams.getInt( "user.param.topnmovies" )

    // ingest data
    val moviesFilePath = paths( "inputPath" ) + "movies.dat"
    val ratingsFilePath = paths( "inputPath" ) + "ratings.dat"
    val moviesData = Ingest.textFile( moviesFilePath )
    val ratingsData = Ingest.textFile( ratingsFilePath )

    // create the base tables in Dataset form
    val movies = Transform.toMovieTable( moviesData, delimiter ).cache
    val ratings = Transform.toRatingsTable( ratingsData, delimiter ).cache

    // create table of movie ratings statistics
    val movieRatings = Generate.movieRatingsTable( hiveContext, moviesData, ratingsData )

    // create table for top N movies of each user
    val topNMoviesPerUser = Generate.topNMoviesPerUserTable( hiveContext, moviesData, ratingsData, topN )

    // persist in parquet format
    val movieRatingsFilePath = paths( "outputPath" ) + "movieRatings.parquet"
    movieRatings.write.parquet( movieRatingsFilePath )
    val topNMoviesPerUserPath = paths( "outputPath" ) + "topNMoviesPerUser.parquet"
    topNMoviesPerUser.write.parquet( topNMoviesPerUserPath )

  }

}
