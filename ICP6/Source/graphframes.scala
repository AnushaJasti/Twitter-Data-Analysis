import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object graphframes {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("GraphAlgorithm")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("GraphAlgorithm")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //importing dataset
    val tr = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("datasets/201508_trip_data.csv")  //change this

    val st = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("datasets/201508_station_data.csv")  //change this

    // Printing the Schema
    tr.printSchema()
    st.printSchema()

    //Temp View
    tr.createOrReplaceTempView("Trips")
    st.createOrReplaceTempView("Stations")

    //total stations and trips
    val station = spark.sql("select * from Stations")
    val trips = spark.sql("select * from Trips")

    //removing duplicates and renaming columns and creating vertices
    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()

    //removing duplicates and renaming columns and creating edges
    val tripEdges = trips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    //create a graphframe with above vertex and edges
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    tripEdges.cache()
    stationVertices.cache()

   // println("Total Number of Stations: " + stationGraph.vertices.count)
   // println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
   // println("Total Number of Trips in Graph: " + stationGraph.edges.count)
   // println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
   // println("Total Number of Trips in Original Data: " + trips.count)//

    //show some vertices and edges
    // stationGraph.vertices.show()
    //stationGraph.edges.show()

    // Triangle Count
    val stTriCount = stationGraph.triangleCount.run()
    stTriCount.select("id","count").show()

    // Shortest Path
    val shortpath = stationGraph.shortestPaths.landmarks(Seq("Japantown","San Jose Civic Center","Santa Clara County Civic Center")).run
    shortpath.select("id","distances").show()
    //shortpath.show(numRows = 30,truncate = false)

    //Page Rank
    val stPgRank = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stPgRank.vertices.select("id", "pagerank").show(10)
    stPgRank.edges.select("src", "dst", "weight").distinct().show(10)
    
    //save graphs generated
    stationGraph.vertices.write.csv("\\vertices")  //location of vertices folder
    stationGraph.edges.write.csv("\\edges")  //location of edges folder

    //Bonus 1 : Label Propagation Algorithm
    val lprop = stationGraph.labelPropagation.maxIter(5).run()
    lprop.orderBy("label").show(10)
    //lprop.orderBy("id").show(10)

    //Bonus 3 : BFS
    val bfs = stationGraph.bfs
      .fromExpr("id = 'Townsend at 7th'")
      .toExpr("id = 'Spear at Folsom'")
      .maxPathLength(2).run()
    bfs.show(10)
    // val pathBFS = stationGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 12").run()
    //pathBFS.show(numRows = 70,truncate = false)

  }
}
