#


./spark-submit --master local\[*\] 
  --files "application.properties,spark.properties" 
  --driver-class-path ./ 
  --conf spark.driver.extraJavaOptions="-DinputPath=/PATH/TO/INPUT/FOLDER 
                                        -DoutputPath=/PATH/TO/OUTPUT/FOLDER
  --class org.aeb.uk.movielens.driver.Main 
  movielens-assembly-1.0.jar