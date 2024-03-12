package DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsExpressions extends App{

  val spark = SparkSession
    .builder
    .appName("DF Columns and Expressions")
    .config("spark.master","local")
    .getOrCreate()

  val carsDf = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDf.show()

  //Columns
  val firstColumn = carsDf.col("name")

  //selecting (projecting)
  val carsNamesDf = carsDf.select(firstColumn)

  carsNamesDf.show()

  //various select methods
  import spark.implicits._
  carsDf.select(
    carsDf.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, //Scala symbol, auto-converted to column
    $"Horsepower", //fancie interpolated string, returns a Column object
    expr("Origin") //Expresion
  )

  //select with plain column names
  carsDf.select("Name", "Year")

  //EXPRESSIONS

  val simplestExpression = carsDf.col("Weight_in_lbs")
  val weightInKgExpression = carsDf.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDf = carsDf.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"), //rename column
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  carsWithWeightsDf.show()

  val carsWithSelectExprWeightDf = carsDf.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //adding a column
  val carsWithKg3Df = carsDf.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDf.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  //careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  //remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDf = carsDf.filter(col("Origin") =!= "USA")
  val europeanCarsDf2 = carsDf.where(col("Origin") =!= "USA")

  //filtering with expression strings
  val americanCarsDf = carsDf.filter("Origin = 'USA'")

  //chain filters
  val americanPowerFulCarsDf = carsDf.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerFulCarsDf2 = carsDf.filter((col("Origin") === "USA") and col("Horsepower") > 150)

  // unioning = adding more rows
  val moreCarsDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDf = carsDf.union(moreCarsDf) //works if the dfs the same schema

  //distinct values
  val allCounstriesDf = carsDf.select("Origin").distinct()
  allCounstriesDf.show()



  val moviesDf = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  val moviesReleaseDf = moviesDf.select("Title","Release_Date")

  val moviesProfitDf = moviesDf.select(
    col("Title"),
    col("US_Gross"),
    col("US_DVD_Sales"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")
  )

  moviesProfitDf.show()

  val atLeastMediocreComediesDf = moviesDf.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  atLeastMediocreComediesDf.show()

}
