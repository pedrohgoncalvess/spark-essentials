package DataFrames

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object DataSources extends App{

  val ss = SparkSession.builder.appName("Data souces and formats").config("spark.master","local").getOrCreate

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

  /*
  Reading DF:
  - format
  - schema (optional)
  - zero or more option
  */

  val carsDf = ss.read
    .format("json")
    .schema(carsSchema) //enforce schema
    //.option("inferSchema","true")
    .option("mode","failFast") //failFast most raise exception on malformed df, dropMalformed will drop column and permissive (default) continue the process
    //.option("path","src/main/resources/data/cars.json")
    .load("src/main/resources/data/cars.json")

  val carsDfWithOptions = ss.read
    .format("json")
    .options(Map(
      "inferSchema" -> "true",
      "path" -> "src/main/resources/data/cars.json",
      "mode" -> "permissive" //default
    )).load()


  //writing Dfs
  /*
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  */

//  carsDfWithOptions.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars.json")

  //JSON FLAGS
  ss.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd") //couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes","true")
    .option("compression", "uncompressed") //bzip2, gzip, snappy, deflate
    .load("src/main/resources/data/cars.json")

  //CSV FLAGS
  val stocksSchema = StructType(Array(
    StructField("symbol",StringType),
    StructField("date", DateType),
    StructField("price",DoubleType)
  ))

  ss.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("nullValue", "")
    .option("sep",",") //default is comma (,)
    .load("src/main/resources/data/stocks.csv")

  //PARQUET FLAGS

//  carsDf.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/cars.parquet")
//.save(path)

  //DATABASE FLAGS

  println("database here")

  val employess = ss.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  employess.show(10)

  val moviesDf = ss.read.json("src/main/resources/data/movies.json")
  moviesDf.write
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
    .save()

}
