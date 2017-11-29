import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object CdrFile {
  val sc = SparkSession.builder().appName("Job").getOrCreate().sparkContext
  val spark = SparkSession.builder().appName("Job").getOrCreate()


  def readFile(s : String) = {

    val lines = scala.io.Source.fromFile(s).getLines().toSeq
    val rdd = spark.sparkContext.parallelize(lines)
    rdd.map(_.split(",", -1))
  }
  def structure (rdd : RDD[Array[String]], br : Broadcast[(String, String, String, String)]) = {
    val array = Array(StructField("client_id", StringType, nullable = false), StructField("display_name", StringType, nullable = false),
      StructField("parent_client_id", StringType, nullable = false), StructField("parent_display_name", StringType, nullable = false)
      ,StructField("country_id", StringType, nullable = false),StructField("year", StringType, nullable = false),
      StructField("month", StringType, nullable = false),StructField("date", StringType, nullable = false))
    val schema = StructType(array)
    val modified = rdd.map(x=> Row(x(0), x(1), x(2), x(3),x(4), br.value._4, br.value._2, br.value._3))
    spark.createDataFrame(modified , schema)
  }
  def writeToFile(df : sql.DataFrame, fileLocation :  String, br : Broadcast[(String, String, String, String)]) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fullPath = fileLocation+"/year=%s/month=%s/date=%s"
    val path = String.format(fullPath,br.value._4,br.value._2, br.value._3 )
    fs.delete(new Path(path), true)
    df.write.mode("append").partitionBy("year","month","date").parquet(fileLocation)
  }
  def main (args : Array[String]) : Unit = {
    val cdrFile = args(1)
    val date = args(0)
    val dateSplit = date.split("/")
    val year = dateSplit(2)
    val month = dateSplit(0)
    val day = dateSplit(1)
    val brDate = sc.broadcast(date, month, day, year)
    val source = readFile(cdrFile)
    val structuredDateFrame = structure(source, brDate)
    structuredDateFrame.show()
    writeToFile(structuredDateFrame, args(2), brDate)
  }

}
