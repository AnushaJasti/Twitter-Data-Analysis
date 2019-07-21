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

    //importing the dataset
    val tr = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("datasets/201508_trip_data.csv")  //change this

    //importing the dataset
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
   
    //concatenation
    val concat = spark.sql("select concat(lat,long) from Stations").show()
    //total staions and trips
    val nstation = spark.sql("select * from Stations")
    val ntrips = spark.sql("select * from Trips")
   
    //removing duplicates and renaming columns and creating vertices 
    val stationVertices = station
      .withColumnRenamed("name", "id")
      .distinct()
    
    //removing du-plicates and renaming columns and creating edges
    val tripEdges = trips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")
    
    //create a graphframe with above vertex and edges
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + trips.count)//
 
    //show some vertices
    stationGraph.vertices.show()
    //show some edges
    stationGraph.edges.show()

    //indegree
    val inDeg = stationGraph.inDegrees
    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)

    //outdegree
    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)

    //motifs
    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()
 
    //Bonus 1 : vertex degree
    val ver = stationGraph.degrees
    ver.show(5)
    
    //Bonus 2 : Common destinations
    val commondest = stationGraph.edges
      .groupBy("src", "dst").count()
      .orderBy(desc("count"))
      .limit(10)
    commondest.show()
    
    //Bonus3 : highest in degrees, fewest out degrees
    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    degreeRatio.cache()
    println(degreeRatio.orderBy(desc("degreeRatio")).limit(10))
    degreeRatio.show()

    //Bonus4 : save graphs generated
    stationGraph.vertices.write.csv("\\vertices")  //location of vertices folder
    stationGraph.edges.write.csv("\\edges")  //location of edges folder

  }
}
