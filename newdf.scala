import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.Row
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.sql.functions.countDistinct
import java.lang.System.setProperty
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark._

object df {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","D:\\winutils" );
    val conf = new SparkConf().setMaster("local[2]").setAppName("My app")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  //  val spark = SQLContext
  //    .builder()
  //    .appName("Spark SQL DataFrames")
  //    .config(conf = conf)
  //    .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // For implicit conversions like converting RDDs to DataFrames
   // import spark.implicits._


    val df = sqlContext.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\thota\\IdeaProjects\\Spark_ICP_3\\survey.csv")


    //Save data to the ouput folder
    df.write.format("csv").option("header","true").save("C:\\Users\\thota\\IdeaProjects\\Spark_ICP_3\\output")


    //Apply Union operation on the dataset and order the output by Country Name alphabetically.
    val df1 = df.limit(5)
    val df2 = df.limit(10)
    df1.show()
    df2.show()
    val unionDf = df1.union(df2)
    println("Union Operation : ")
    unionDf.orderBy("Country").show()

    df.registerTempTable("survey")


    // Duplicate Records

    val DupDF = sqlContext.sql("select COUNT(*),Country from survey GROUP By Country Having COUNT(*) > 1")
    DupDF.show()



    //Use Groupby Query based on treatment

    val treatment = sqlContext.sql("select count(Country) from survey GROUP BY treatment ")
    println("Group by treatment : ")
    treatment.show()


    //Aggregate Max and Average

    val MaxDF = sqlContext.sql("select Max(Age) from survey")
    println("Maximum of age : ",MaxDF.show())
    //  MaxDF.show()

    val AvgDF = sqlContext.sql("select Avg(Age) from survey")
    println("Average of age : ",AvgDF.show())
    //  AvgDF.show()



    //Join the dataframe using sql

    val df3 = df.select("Country","state","Age","Gender","Timestamp")
    val df4 = df.select("self_employed","treatment","family_history","Timestamp")
    df3.registerTempTable( "left")
    df4.registerTempTable("right")

    val joinSQl = sqlContext.sql("select left.Gender,right.treatment,left.state,right.self_employed FROM left,right where left.Timestamp = " +
      "right.Timestamp")
    joinSQl.show(numRows = 50)



    //Fetch 13th row from DataFrame

    val df13th = df.take(13).last
    println("13th row of dataset : ")
    print(df13th)



    //Bonus Question

    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val Country = fields(3).toString
      val  state = fields(4).toString
      val  Gender = fields(2).toString
      (Country,state,Gender)
    }
    val lines = sc.textFile("C:\\Users\\thota\\IdeaProjects\\Spark_ICP_3\\survey.csv")
    val rdd = lines.map(parseLine).toDF()
    println("")
    println("After ParseLine method : ")
    rdd.show()


  }
}
