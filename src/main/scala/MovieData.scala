import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object MovieDataJob extends App {
  /**
   * creating spark session
   */

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]").appName("testApp")
    .getOrCreate()

  import spark.implicits._

  /**
   * This case class used to crate schema for the file
   * @param index
   * @param budget
   * @param genres
   * @param homepage
   * @param id
   * @param keywords
   * @param original_language
   * @param original_title
   * @param overview
   * @param popularity
   * @param production_companies
   * @param production_countries
   * @param release_date
   * @param revenue
   * @param runtime
   * @param spoken_languages
   * @param status
   * @param tagline
   * @param title
   * @param vote_average
   * @param vote_count
   * @param cast
   * @param crew
   * @param director
   */
  case class MovieData(index: Int,
                       budget: Float,
                       genres: String,
                       homepage: String,
                       id: String,
                       keywords: String,
                       original_language: String,
                       original_title: String,
                       overview: String,
                       popularity: Float,
                       production_companies: String,
                       production_countries: String,
                       release_date: String,
                       revenue: Float,
                       runtime: Float,
                       spoken_languages: String,
                       status: String,
                       tagline: String,
                       title: String,
                       vote_average: String,
                       vote_count: String,
                       cast: String,
                       crew: String,
                       director: String) {

  }

  val movieDataSchema = Encoders.product[MovieData].schema
  /**
   * creating data frame csv
   */
  val myDf = spark.read.
    option("multiLine", "true")
    .option("escape", "\"")
    .option("header", "true")
    .schema(movieDataSchema).csv("D:\\files\\movie_dataset.csv")

  myDf.cache()

  /**
   * titlesWithMnRunTime.show() gives movie(s) with the smallest runtime
   * titlesWithMxRunTime.show(1000) gives movies with maximum run time
   */

  val minAndMaxRunTimeArr = myDf.filter($"runtime".isNotNull).select(min($"runtime"),max($"runtime")).collect()(0)
  val minRunTime =minAndMaxRunTimeArr(0)
  val maxRunTime =minAndMaxRunTimeArr(1)
  val titlesWithMnRunTime = myDf.select($"title", $"runtime").filter($"runtime".isNotNull).where($"runtime".equalTo(minRunTime))
  val titlesWithMxRunTime = myDf.select($"title", $"runtime").filter($"runtime".isNotNull).where($"runtime".equalTo(maxRunTime))

   titlesWithMnRunTime.show(1000)
   titlesWithMxRunTime.show(1000)

  System.exit(0)

  /**
   * This case class is used to create schema for product_companies column
   * @param name: production house name
   * @param id :production house id
   */

  case class ProductionCompany(
                                val name: String,
                                val id: Int) {
  }

  val schema = Encoders.product[ProductionCompany].schema

  /**
   * Using we can extract the production house data with using from_json over production_companies column
   */
  val dfProductionHouse = myDf.filter(trim($"production_companies").notEqual(lit("[]"))).select($"budget", $"title", $"popularity", $"vote_average", $"revenue", $"release_date",
    from_json(col("production_companies"), schema).as("jsonData"))
    .select("budget", "title", "popularity", "vote_average", "revenue", "release_date", "jsonData.*")

  dfProductionHouse.cache()

  val idWithMaxBudget = dfProductionHouse.groupBy($"id").max("budget").toDF("id", "max_budget")
  val idWithMaxBudgetWindow = Window.orderBy(desc("max_budget"))
  val top5ProductionHouses = idWithMaxBudget.withColumn("rank", rank().over(idWithMaxBudgetWindow)).filter($"rank".leq(5))
    .select($"id".as("id1"), $"max_budget", $"rank".as("rank_prod"))

  val moviesOfTop5prod = dfProductionHouse.join(top5ProductionHouses, dfProductionHouse.col("id").equalTo(top5ProductionHouses.col("id1")), "inner")

  val popularityWindow = Window.partitionBy("id").orderBy(desc("popularity"))
  val top5MoviesOfTop5ProdHouse = moviesOfTop5prod.withColumn("rank_movie", rank().over(popularityWindow)).filter($"rank_movie".leq(5))
    .select("rank_prod", "id", "name", "budget", "rank_movie", "popularity", "title", "vote_average", "revenue")
    .orderBy("id", "rank_movie")

  //top5MoviesOfTop5ProdHouse.show(10000)


  /**
   * this methods gives data between two years
   * @param lowerBound :
   * @param upperBound
   * @param df
   * @return
   */

  def getYearDataForYearRange(lowerBound: Int, upperBound: Int, df: DataFrame) = {
    df.withColumn("year", $"release_date".substr(0, 4).cast(IntegerType)).
      filter($"year".between(lowerBound, upperBound))
  }

  /**
   * prodHouseMaxFilmsInYear.show gives production houses which produced most number of films year wise (2000 -2016)
   */
  val dfFrom2000to2016 = getYearDataForYearRange(2000, 2016, dfProductionHouse)

  val prodHouseWithNoOfFilms = dfFrom2000to2016.groupBy($"name", $"year").agg(count($"title")).orderBy("year", "name").toDF("production_house", "year", "no_films")

  val yearWithMaxFilms = prodHouseWithNoOfFilms.groupBy("year").agg(max($"no_films")).toDF("year1", "max_films")

  val prodHouseMaxFilmsInYear = yearWithMaxFilms.
    join(prodHouseWithNoOfFilms, prodHouseWithNoOfFilms.col("year").equalTo(yearWithMaxFilms.col("year1")).and(
      prodHouseWithNoOfFilms.col("no_films").equalTo(yearWithMaxFilms.col("max_films")
      )), "inner").orderBy("year")
    .select("production_house", "year", "no_films")

  //prodHouseMaxFilmsInYear.show(1000)

  /**
   * In order to analyse Marvel Studios and DC Comics for investment purpose, I calculated
   * the total profit (revenue -budget) for each movie and calculated the total profit year wise (2000-2020)
   * then plotted graph for the same
   * Since Marvel studio gives the consistant profit , I will go ahead with Marvel Studio
   *
   *
   *
   */

  val dataOfMarvNDC = getYearDataForYearRange(2000, 2020, dfProductionHouse).filter($"id".equalTo(420).or($"id".equalTo(429)))
  val profitOfOfMarvNDC = dataOfMarvNDC.withColumn("profit", $"revenue".-($"budget"))
  val yearWiseProfit =profitOfOfMarvNDC.groupBy("name", "year").sum("profit").orderBy("year")
  // yearWiseProfit.show(1000)

}

