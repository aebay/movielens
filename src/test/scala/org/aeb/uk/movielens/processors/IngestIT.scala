package org.aeb.uk.movielens.processors

import org.aeb.uk.movielens.models.TextFileGenerator
import org.aeb.uk.spark.SparkHiveSuite

class IngestIT extends SparkHiveSuite {

  private val colonDelimitedFile = TextFileGenerator( "/tmp/", "colonDelimitedText.txt", delimiter = ":" )

  // Todo: seems awkward, must be a better way of doing this...
  private lazy val hiveContextImplicit = hiveContext

  override def beforeAll(): Unit = {

    super.beforeAll()

    TextFileGenerator.write( colonDelimitedFile )

  }

  behavior of "Ingestion module"

  it should "read in a text file and convert each line to a continuous string element" in {

    import hiveContextImplicit.implicits._

    val row = "test1:test2:test3"
    val input = sparkContext.parallelize( Array(row, row, row) )

    val expectedResult = hiveContext.createDataset( input )
    val actualResult = Ingest.textFile( colonDelimitedFile.filePath )

    assert( expectedResult.collect.deep === actualResult.collect.deep )

  }

  override def afterAll(): Unit = {

    TextFileGenerator.delete( colonDelimitedFile )

    super.afterAll()

  }

}