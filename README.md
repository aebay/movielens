# movielens

Application that reads data from the movielens database and outputs parquet tables for the following:

1. Movie data with minimum, maximum and average ratings given by all users.
2. Top 3 films, by rating, determined by each user.
  - in the event that there are more than 3 films that have the same top rating, the top 3 by movie ID in ascending order are returned.
  - the top N films per user can be returned by changing the value of `user.param.topnmovies` in `application.properties`

## Configuration

### Source files

1.  Download the movielens database files from [http://files.grouplens.org/datasets/movielens/ml-1m.zip]
2.  Unzip the archive. The unzipped archive folder will be referenced as `/PATH/TO/INPUT/FOLDER`

### JAR

**Note**: This build was made using SBT v0.13.16.

1.  Clone the project from this repository.
2.  Add `project/assembly.sbt` to the project.
3.  Add `addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")` to `project/assembly.sbt`
4.  From the terminal, in the root of the project run:
```
$ sbt clean clean-files assembly
```

## Running

**Note**: This project has only been configured to run in Spark local mode.  It assumes that `spark-submit` is on your classpath.
 
1.  Copy `target/scala-2.10/movielens-assembly-1.0.jar` to a working directory of your choice.
2.  Copy `src/main/resources/*.properties` to the same working directory as (1).
3.  Run the following `spark-submit` command:
```
./spark-submit --master local[*] \ 
               --files "application.properties,spark.properties" \ 
               --driver-class-path ./ \ 
               --conf spark.driver.extraJavaOptions="-DinputPath=/PATH/TO/INPUT/FOLDER \ 
                                                     -DoutputPath=/PATH/TO/OUTPUT/FOLDER \
               --class org.aeb.uk.movielens.driver.Main \
               movielens-assembly-1.0.jar
```

**Notes**:

`/PATH/TO/INPUT/FOLDER` is the absolute path to the input data folder and should contain `movies.dat` and `ratings.dat`

`/PATH/TO/OUTPUT/FOLDER` is the absolute path to the output data folder.  It should *not* exist before running the application, otherwise the application will fail.
