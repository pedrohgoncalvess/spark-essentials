package DataFrames

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Basics extends App {

  //creating a spark session
  val spark = SparkSession.builder
    .appName("DataFrames Basics")
    .config("spark.master","local")
    .getOrCreate

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()
  firstDF.printSchema()

  //get rows
  firstDF.take(10).foreach(println)

  //spark types
  val longType = LongType

  //schema
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", IntegerType),
      StructField("Cylinders",IntegerType),
      StructField("Displacement", IntegerType),
      StructField("HorsePower",IntegerType),
      StructField("Weight_in_lbs",IntegerType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  //obtain a schema
  val carsDFSchema = firstDF.schema

  // read a DF with your schema
  val carsWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  val myRow = Seq(("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"))

  val carsWithoutSchema = spark.createDataFrame(myRow) //schema auto-inferred

  //create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithTheImplicits = myRow.toDF("Name","MPG","Cylinders","Displacement","HP","Weight","Acceleration","Year", "CountryOrigin")

  carsWithoutSchema.printSchema()
  manualCarsDFWithTheImplicits.printSchema()

}
