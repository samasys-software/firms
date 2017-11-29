import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SdrFile {
  val sc = SparkSession.builder().appName("Job").getOrCreate().sparkContext;
  val spark = SparkSession.builder().appName("Job ").getOrCreate();


  def readFile(s : String) = {
    val rdd = sc.textFile(s)
    rdd.map(_.split(",",-1))
  }

  def structure (rdd : RDD[Array[String]], br : Broadcast[(String, String, String, String)]) = {
    val source = rdd.filter(x=> !x(0).startsWith("issue_name"))
    val array = Array(StructField("issue_name", StringType, nullable = false),
      StructField("sdr_id", StringType, nullable = false),
      StructField("description", StringType, nullable = false),
      StructField("issuer_bloomberg_id", StringType, nullable = false),
      StructField("issuer_name", StringType, nullable = false),
      StructField("issue_currency", StringType, nullable = false),
      StructField("coupon_maturity_date", StringType, nullable = false),
      StructField("coupon_rate", StringType, nullable = false),
      StructField("isin", StringType, nullable = false),
      StructField("cusip", StringType, nullable = false),
      StructField("bloomberg", StringType, nullable = false),
      StructField("fitch_it_rating", StringType, nullable = false),
      StructField("fitch_st_rating", StringType, nullable = false),
      StructField("dbrs_lt_rating", StringType, nullable = false),
      StructField("dbrs_st_rating", StringType, nullable = false),
      StructField("s_p_lt_rating", StringType, nullable = false),
      StructField("s_p_st_rating", StringType, nullable = false),
      StructField("moodys_it_rating", StringType, nullable = false),
      StructField("moodys_st_rating", StringType, nullable = false),
      StructField("issuer_country_inc", StringType, nullable = false),
      StructField("issuer_country_risk", StringType, nullable = false),
      StructField("collable", StringType, nullable = false),
      StructField("collable_date", StringType, nullable = false),
      StructField("index_ratio", StringType, nullable = false),
      StructField("collateral_type", StringType, nullable = false),
      StructField("inflating_linked", StringType, nullable = false),
      StructField("product_sub_type", StringType, nullable = false),
      StructField("product_type", StringType, nullable = false),
      StructField("sinkable", StringType, nullable = false),
      StructField("issue_type", StringType, nullable = false),
      StructField("issuer_rating", StringType, nullable = false),
      StructField("date", StringType, nullable = false),
      StructField("month", StringType, nullable = false),
      StructField("year", StringType, nullable = false))
    val schema = StructType(array);
    val modified = source.map(x=> Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7),
      x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28),x(29),x(30), br.value._4, br.value._2, br.value._3));
    spark.createDataFrame(modified , schema);

  }
  def writeToFile(df : sql.DataFrame, fileLocation : String) = {
    df.show()
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path (fileLocation), true)
    df.write.parquet(fileLocation)


  }
  def main (args : Array[String]) : Unit = {

    val date = args(0);
    val dateSplit = date.split("/");
    val year = dateSplit(2);
    val month = dateSplit(0);
    val day = dateSplit(1);
    val brDate = sc.broadcast(date, month, day, year);
    val SDRFile = args(1)+"/year=%s/month=%s/date=%s/";
    val path = String.format(SDRFile,year, month, day)
    val source = readFile(path);
    println(source.count());
    val structuredDateFrame = structure(source, brDate);
    structuredDateFrame.show();
    writeToFile(structuredDateFrame,args(2) );
  }

}