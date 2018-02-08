package org.aeb.uk.movielens

import org.aeb.uk.models.TextFileGenerator
import org.aeb.uk.movielens.processors.Ingestion
import org.aeb.uk.spark.SparkHiveSuite

class IngestionIT extends SparkHiveSuite {

  private val colonDelimitedFile = TextFileGenerator( "/tmp/", "colonDelimitedText.txt", delimiter = ":" )
  private val spaceDelimitedFile = TextFileGenerator( "/tmp/", "spaceDelimitedText.txt" )

  // Todo: seems awkward, must be a better way of doing this...
  private lazy val hiveContextImplicit = hiveContext

  override def beforeAll(): Unit = {

    super.beforeAll()

    TextFileGenerator.write( colonDelimitedFile )
    TextFileGenerator.write( spaceDelimitedFile )

  }

  behavior of "Ingestion module"

  it should "read in a text file and split on the ':' delimiter" in {

    import hiveContextImplicit.implicits._

    val row = Array( "test1", "test2", "test3" )
    val input = sparkContext.parallelize( Array(row, row, row) )

    val expectedResult = hiveContext.createDataset( input )
    val actualResult = Ingestion.readAsDataset( colonDelimitedFile.filePath, colonDelimitedFile.delimiter )

    assert( expectedResult.collect.deep === actualResult.collect.deep )

  }

  it should "read in a text file and split on the ' ' delimiter" in {

    import hiveContextImplicit.implicits._

    val row = Array( "test1", "test2", "test3" )
    val input = sparkContext.parallelize( Array(row, row, row) )

    val expectedResult = hiveContext.createDataset( input )
    val actualResult = Ingestion.readAsDataset( spaceDelimitedFile.filePath, spaceDelimitedFile.delimiter )

    assert( expectedResult.collect.deep === actualResult.collect.deep )

  }

  override def afterAll(): Unit = {

    TextFileGenerator.delete( colonDelimitedFile )
    TextFileGenerator.delete( spaceDelimitedFile )

    super.afterAll()

  }

}