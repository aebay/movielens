package org.aeb.uk.movielens.models

import java.io.PrintWriter
import scala.sys.process._

/**
  * Class that holds the variables required to generate a text file.
  *
  * Text file operations are controlled via the companion object.
  *
  * @param path
  * @param name
  * @param delimiter
  * @param words
  * @param lines
  * @param testWord
  */
case class TextFileGenerator( path: String,
                              name: String,
                              delimiter: String = " ",
                              words: Int = 3,
                              lines: Int = 3,
                              testWord: String = "test" ) {

  def filePath = path + name

}

object TextFileGenerator {

  def write( t: TextFileGenerator ): Unit = {

    val out = new PrintWriter( t.path + t.name )
    for ( i <- 1 to t.lines; j <- 1 to t.words ) {
      out.print( s"${t.testWord}$j" )
      if ( j != t.words ) out.print( s"${t.delimiter}" )
      else out.println
    }
    out.close()

  }

  def delete( t: TextFileGenerator ): Unit = {

    s"rm -rf ${t.filePath}".!

  }

}