import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.sql.functions.countDistinct



object df {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\winutils" );  //change this
    val conf = new SparkConf().setMaster("local[2]").setAppName("My app")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL DataFrames")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Lenovo\\IdeaProjects\\M2_ICP3\\survey.csv")  //change this


    //Save data to the ouput folder
    df.write.format("csv").option("header","true").save("C:\\Users\\Lenovo\\IdeaProjects\\M2_ICP3\\output")  //change this


    //Apply Union operation on the dataset and order the output by Country Name alphabetically.
    val df1 = df.limit(5)
    val df2 = df.limit(10)
    df1.show()
    df2.show()
    val unionDf = df1.union(df2)
    println("Union Operation : ")
    unionDf.orderBy("Country").show()

    df.createOrReplaceTempView("survey")


    // Duplicate Records

    val DupDF = spark.sql("select COUNT(*),Country from survey GROUP By Country Having COUNT(*) > 1")
    DupDF.show()



    //Use Groupby Query based on treatment

    val treatment = spark.sql("select count(Country) from survey GROUP BY treatment ")
    println("Group by treatment : ")
    treatment.show()


    //Aggregate Count, Max and Average
    
    val CountDF= sqlContext.sql("select COUNT(Age) from survey")
    println("Count of age : ",CountDF.show())
   // CountDF.show()
    
    val MaxDF = spark.sql("select Max(Age) from survey")
    println("Maximum of age : ",MaxDF.show())
  //  MaxDF.show()

    val AvgDF = spark.sql("select Avg(Age) from survey")
    println("Average of age : ",AvgDF.show())
  //  AvgDF.show()



    //Join the dataframe using sql
    
    val df3 = df.select("Country","state","Age","Gender","Timestamp")
    val df4 = df.select("self_employed","treatment","family_history","Timestamp")
    df3.createOrReplaceTempView( "left")
    df4.createOrReplaceTempView("right")

    val joinSQl = spark.sql("select left.Gender,right.treatment,left.state,right.self_employed FROM left,right where left.Timestamp = " +
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
    val lines = sc.textFile("/Users/anushamuppalla/Downloads/survey.csv")  //change this
    val rdd = lines.map(parseLine).toDF()
    println("")
    println("After ParseLine method : ")
    rdd.show()
    
    
  }
}
