import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Sqoop {
  val sc= SparkSession.builder().appName("sqoop").getOrCreate().sparkContext;

  val spark = SparkSession.builder().appName("sqoop").getOrCreate()
  def readFile(s :String) = {
    val file = sc.textFile(s);
    file.map(_.split(","))
  }
  def structure (rdd: RDD[Array[String]]) = {
    val array = Array(StructField("country_id", StringType, nullable = false), StructField("country_name", StringType, nullable = false),
      StructField("isd_code", StringType, nullable = false), StructField("currency_name", StringType, nullable = false),
      StructField("currency_symbol", StringType, nullable = false))
    val schema = StructType(array)
    val modified = rdd.map(x=> Row(x(0), x(1), x(2), x(3), x(4)))
    spark.createDataFrame(modified, schema)
  }
  def writeToFile(frame: sql.DataFrame, location: String) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(location), true)
    frame.write.parquet(location)
  }



  def main(args: Array[String]): Unit = {
    val file = args(0);
    val source = readFile(file);
    val df = structure(source)
    writeToFile(df, args(1))
  }


}
