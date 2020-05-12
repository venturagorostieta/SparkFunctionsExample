# SparkFunctionsExample
Example using spark 2.3 and Functions. Loading log file for test app


Example for run application with Spark:


spark-submit \
  --class "com.example.function.FunctionExample" \
  --master local[*] \
 /tmp/spark/SparkFunctions.jar  /tmp/examples/spark/functions/guests.csv   /tmp/examples/spark/functions/adultMinor /tmp/examples/spark/functions/agregateFamily
