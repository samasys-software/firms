import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql
import org.apache.spark.sql._


object MonacoFileToStd{
  val sc = SparkSession.builder().appName("Job").getOrCreate().sparkContext
  val spark = SparkSession.builder().appName("Job").getOrCreate()


  def readFromSrc(s:String) = {
    val df= spark.read.parquet(s)
    df.createOrReplaceTempView("monaco_file")
    val rdd = spark.sql("select * from monaco_file").filter(x=> (!x(0).equals("security_identifier_flag")))
    rdd
  }

  def writeToStd(df : sql.DataFrame, location :String ) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(location), true)
    df.write.parquet(location)
  }
  def dateValues (dateString : String) ={
    val date = dateString
    val dateSplit = date.split("/")
    val year = dateSplit(2)
    val month = dateSplit(0)
    val day = dateSplit(1)
    Array(date , year, month , day)
  }


  def main(args: Array[String]): Unit = {
    val brArray = dateValues(args(0))
    var readLocation = args(1)+"/year=%s/month=%s/date=%s/"
    readLocation = String.format(readLocation, brArray(1), brArray(2) , brArray(3))
    val dfFromSrc = readFromSrc(readLocation);
    writeToStd(dfFromSrc, args(2))
  }
}
