package com.manojmallela.spark


import org.apache.http.client.methods.HttpGet
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.jsoup.Jsoup

object Top100 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(Top100.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val top100URL = "http://www.bbc.com/culture/story/20170821-the-100-greatest-comedies-of-all-time"
    val htmlPageContent = Jsoup.connect(top100URL).get().body().text()
    var movies = htmlPageContent.substring(htmlPageContent.indexOf("100. (tie)"))
    movies = movies.substring(movies.indexOf("100. (tie)"), movies.indexOf("Read more about BBC Culture’s")).replace("(tie) ", "")
    val movieData = movies.split("\\) ").toList.map(x => x + "year")

    import spark.implicits._
    val rawMoviesDF = spark.sparkContext.parallelize(movieData).toDF("movie")

    val moviesDF = rawMoviesDF
      .withColumn("year", regexp_extract(regexp_extract($"movie", "\\d{4}year", 0), "\\d{4}", 0).cast("INT"))
      .withColumn("Rank", regexp_extract($"movie", "\\d{2,3}\\.", 0).cast("INT"))
      .withColumn("Title", regexp_extract($"movie", "\\. [^(]+", 0))
      .withColumn("Title", regexp_replace($"Title", "\\. ", ""))
      .withColumn("Title", trim($"Title"))

    val metadataRDD = moviesDF.select("Title", "year").rdd.mapPartitions { parts =>
      val httpClient = HttpClients.createDefault()
      parts.map(x => Utils.getMetadata(httpClient, x.getString(0), x.getInt(1)))
    }

    val metadataDF = spark.read.json(metadataRDD).withColumnRenamed("title", "Title")

    var movieWithInfo = moviesDF.join(metadataDF, Seq[String]("Title", "year"))

    val noResultList = moviesDF
      .select("Title")
      .except(movieWithInfo.select("Title"))
      .map(_.getString(0))
      .collect.toList

    // Titles with no missing metadata
    val noResultDF = moviesDF.filter(x => noResultList.contains(x.getString(3)))

    // Diluted search
    val retryRDD = noResultDF.select("Title", "year").rdd.mapPartitions { parts =>
      val httpClient = HttpClients.createDefault()
      parts.map { x =>
        println(s"Requesting Movie: ${x.getString(0)}, Year: ${x.getInt(1)}")
        Utils.getMetadata(httpClient, x.getString(0), x.getInt(1))
      }
    }

    // Make title simple (dilute) to compensate for inconsistencies in Request and Response Title.
    val makeSimple: String => String = _.filter(_.isLetterOrDigit).toLowerCase
    val udfMakeSimple = udf(makeSimple(_: String))

    val retryDF = spark.read.json(retryRDD)
      .withColumn("title", trim($"title"))
      .withColumn("year", $"year".cast("INT"))
      .withColumn("simpleTitle", udfMakeSimple($"title"))
      .drop("title", "Title")

    var combineMetadata2 = retryDF.join(
      noResultDF.withColumn("simpleTitle", udfMakeSimple($"Title")), Seq[String]("simpleTitle", "year"))
      .drop("simpleTitle")

    val incompatibleFields = Utils.compareSchema(combineMetadata2.schema, movieWithInfo.schema)
    println("Incompatible Fields: " + incompatibleFields.mkString("\t"))

    combineMetadata2 = combineMetadata2.withColumn("mon_id", monotonically_increasing_id)
    movieWithInfo = movieWithInfo.withColumn("mon_id", monotonically_increasing_id)

    val finalDF = combineMetadata2.join(movieWithInfo, Seq("Title"), "fullouter")


  }
}

object Utils {

  def getMetadata(httpClient: CloseableHttpClient, movieTitle: String, releaseYear: Int): String = {

    val url = s"http://www.theimdbapi.org/api/find/movie?title='$movieTitle'&year=$releaseYear".replaceAll(" ", "%20")
    val httpGet = new HttpGet(url)
    val context = HttpClientContext.create()
    val response = httpClient.execute(httpGet, context)
    val result = EntityUtils.toString(response.getEntity)
    result
  }

  def compareSchema(schema1: StructType, schema2: StructType): List[String] = {
    val schema1Fields = schema1.map(_.name).sorted
    val schema2Fields = schema2.map(_.name).sorted
    schema1Fields
      .union(schema2Fields)
      .diff(schema2Fields.union(schema1Fields))
      .toList
  }
}
