package org.aeb.uk.models

import org.aeb.uk.movielens.models.Movie;
import org.scalatest.FlatSpec;

class MovieTest extends FlatSpec {

    private val movieId = 1
    private val title = "Deadpool"
    private val genres = "action,comedy"

    private val expectedResult = Movie( movieId, title, genres )

    behavior of "Movie class and object"

    it should "instantiate a new Movie class from a tokenised String" in {

        val input = Array( movieId.toString, title, genres )

        val actualResult = Movie( input )

        assert( actualResult === expectedResult )

    }

    it should "instantiate a new Movie class from a String containing a ':' delimiter" in {

        val input = s"$movieId $title $genres"

        val actualResult = Movie( input, ":" )

        assert( actualResult === expectedResult )

    }

    it should "instantiate a new Movie class from a String containing a ' ' delimiter" in {

        val input = s"$movieId $title $genres"

        val actualResult = Movie( input, " " )

        assert( actualResult === expectedResult )

    }

}