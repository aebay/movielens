package org.aeb.uk.movielens.processors

import org.aeb.uk.movielens.models.{Movie, Rating}
import org.aeb.uk.spark.SparkHiveSuite

class TransformIT extends SparkHiveSuite {

  // Todo: seems awkward, must be a better way of doing this...
  private lazy val hiveContextImplicit = hiveContext

  behavior of "Transform module"

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

}