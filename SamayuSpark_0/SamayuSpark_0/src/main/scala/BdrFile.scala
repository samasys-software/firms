import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object BdrFile {
  val sc = SparkSession.builder().appName("Job").getOrCreate().sparkContext
  val spark = SparkSession.builder().appName("Job").getOrCreate()
  def readFile(s : String) = {
    sc.textFile(s)
  }
  def structure (rdd : RDD[String] , broadcast: Broadcast[(String, String , String , String)] ) = {
    val source = rdd.map(_.split(",", -1)).filter(x=> !x(0).startsWith("book_id"))
    val array = Array (StructField("book_id", StringType , nullable = false), StructField("book_name", StringType , nullable = false),
      StructField("client_id", StringType , nullable = false), StructField("year", StringType , nullable = false),
      StructField("month", StringType , nullable = false), StructField("date", StringType , nullable = false))
    val schema = StructType(array)
    val modified = source.map(x => Row(x(0), x(1), x(2),broadcast.value._4, broadcast.value._2, broadcast.value._3))
    spark.createDataFrame(modified, schema)
  }

  def writeToFile(df : sql.DataFrame, fileLocation :  String) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(fileLocation), true)
    df.write.parquet(fileLocation)


  }
  def broadcastValues (dateString : String) ={
    val date = dateString
    val dateSplit = date.split("/")
    val year = dateSplit(2)
    val month = dateSplit(0)
    val day = dateSplit(1)
    Array(date , year, month , day)
  }
  def main (args : Array[String]) : Unit = {
    val brArray = broadcastValues(args(0))
    val brDate = sc.broadcast(brArray(0), brArray(2), brArray(3), brArray(1))
    var readLocation = args(1)+"/year=%s/month=%s/date=%s/"
    readLocation = String.format(readLocation, brArray(1), brArray(2) , brArray(3))
    val source = readFile(readLocation)
    val df = structure(source , brDate)
    df.show()
    writeToFile(df , args(2))
    val data = spark.read.parquet(args(2))
    data.show()
  }

}
