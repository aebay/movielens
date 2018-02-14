package org.aeb.uk.movielens.models

import org.aeb.uk.movielens.models.Rating
import org.scalatest.FlatSpec

class RatingTest extends FlatSpec {

    private val userId = 1
    private val movieId = 2
    private val rating = 3
    private val timestamp = 123456789L

    private val expectedResult = Rating( userId, movieId, rating, timestamp )

    behavior of "Rating class and object"

    it should "instantiate a new Rating class from a tokenised String" in {

        val input = Array( userId.toString, movieId.toString, rating.toString, timestamp.toString )

        val actualResult = Rating( input )

        assert( actualResult === expectedResult )

    }

    it should "instantiate a new Rating class from a String containing a ':' delimiter" in {

        val input = s"$userId:$movieId:$rating:$timestamp"

        val actualResult = Rating( input, ":" )

        assert( actualResult === expectedResult )

    }

    it should "instantiate a new Rating class from a String containing a ' ' delimiter" in {

        val input = s"$userId $movieId $rating $timestamp"

        val actualResult = Rating( input, " " )

        assert( actualResult === expectedResult )

    }

}