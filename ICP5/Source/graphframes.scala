import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object graphframes {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val t = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushamuppalla/Desktop/SparkGraphframe/datasets/201508_trip_data.csv")  //change this

    val s = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushamuppalla/Desktop/SparkGraphframe/datasets/201508_station_data.csv")  //change this



    // Printing the Schema

    t.printSchema()

    s.printSchema()

    

    //First of all create three Temp View

    t.createOrReplaceTempView("Trips")

    s.createOrReplaceTempView("Stations")


    
    //total stations and trip places
    val station = spark.sql("select * from Stations")

    val trips = spark.sql("select * from Trips")


    
    //removing duplicates
    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()


    
    //renaming columns
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


    
    //show some vertices and edges
    stationGraph.vertices.show()

    stationGraph.edges.show()


    

   //indegree---incoming edges
    val inDeg = stationGraph.inDegrees

    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)

    //outgoing edges
    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)




    //motifs---internal pattern
    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")

    motifs.show()



    //BONUS--VERTIX degree
    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))


    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    degreeRatio.cache()


    println(degreeRatio.orderBy(desc("degreeRatio")).limit(10))
    degreeRatio.show()


    val topTrips = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    topTrips.show()




    stationGraph.vertices.write.csv("/Users/anushamuppalla/Desktop/SparkGraphframe/vertices")

    stationGraph.edges.write.csv("/Users/anushamuppalla/Desktop/SparkGraphframe/edges")

  }
}
