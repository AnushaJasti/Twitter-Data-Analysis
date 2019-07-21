import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object icp6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("GraphAlgo")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("GraphAlgo")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushamuppalla/Desktop/SparkGraphframe/datasets/201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushamuppalla/Desktop/SparkGraphframe/datasets/201508_station_data.csv")



    // Printing the Schema

    trips_df.printSchema()

    station_df.printSchema()



    //First of all creat three Temp View

    trips_df.createOrReplaceTempView("Trips")

    station_df.createOrReplaceTempView("Stations")


    val station = spark.sql("select * from Stations")

    val trips = spark.sql("select * from Trips")

    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = trips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")


    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + trips.count)//

    stationGraph.vertices.show()

    stationGraph.edges.show()


    // Triangle Count

    val stationTraingleCount = stationGraph.triangleCount.run()
    stationTraingleCount.select("id","count").show()

    // Shortest Path
    val shortPath = stationGraph.shortestPaths.landmarks(Seq("Japantown","San Jose Civic Center","Santa Clara County Civic Center")).run
    shortPath.show(numRows = 30,truncate = false)

    //Page Rank

    val stationPageRank = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()

    //page rank for particular
    val stationPageRank1 = stationGraph.pageRank.resetProbability(0.15).maxIter(12).sourceId("Japantown")run()
    stationPageRank1.vertices.show()

    // BFS

    val pathBFS = stationGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 12").run()
    pathBFS.show(numRows = 70,truncate = false)

   //label
   val result = stationGraph.labelPropagation.maxIter(5).run()
    result.orderBy("id").show()





  }

}