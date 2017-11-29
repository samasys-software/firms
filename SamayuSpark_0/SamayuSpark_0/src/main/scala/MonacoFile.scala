
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object MonacoFile {
  val sc = SparkSession.builder().appName("Job").getOrCreate().sparkContext;
  val spark = SparkSession.builder().appName("Job ").getOrCreate();
  def readFile(s : String) = {
    val lines = scala.io.Source.fromFile(s).getLines().toSeq;
    val rdd = spark.sparkContext.parallelize(lines);
    rdd.map(_.split(",",-1));
  }
  def structure (rdd : RDD[Array[String]], br : Broadcast[(String, String, String, String)]) = {
    val array = Array(StructField("security_identifier_flag", StringType, nullable = false),
      StructField("firm_identifier", StringType, nullable = false),
      StructField("book_name", StringType, nullable = false),
      StructField("currency", StringType, nullable = false),
      StructField("security_discription", StringType, nullable = false),
      StructField("net_full_open_position", StringType, nullable = false),
      StructField("full_current_net_position", StringType, nullable = false),
      StructField("bid_price", StringType, nullable = false),
      StructField("ask_price", StringType, nullable = false),
      StructField("bid_yield", StringType, nullable = false),
      StructField("ask_yield", StringType, nullable = false),
      StructField("full_original_face_net_position", StringType, nullable = false),
      StructField("unrealized_total_profit_loans", StringType, nullable = false),
      StructField("realized_total_profit_loans", StringType, nullable = false),
      StructField("isin", StringType, nullable = false),
      StructField("cusip", StringType, nullable = false),
      StructField("bloomberg", StringType, nullable = false),
      StructField("avg_cost", StringType, nullable = false),
      StructField("settle_date_long_position", StringType, nullable = false),
      StructField("settle_date_short_position", StringType, nullable = false),
      StructField("buy_trades", StringType, nullable = false),
      StructField("sell_trades", StringType, nullable = false),
      StructField("total_trades", StringType, nullable = false),
      StructField("collateral_type", StringType, nullable = false),
      StructField("previous_coupon_settledate", StringType, nullable = false),
      StructField("next_coupon_settledate", StringType, nullable = false),
      StructField("current_coupon", StringType, nullable = false),
      StructField("contract_size", StringType, nullable = false),
      StructField("transaction_systemdate", StringType, nullable = false),

      StructField("year", StringType, nullable = false),
      StructField("month", StringType, nullable = false),
      StructField("date", StringType, nullable = false))
    val schema = StructType(array);
    val modified = rdd.map(x=> Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7),
      x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16),x(17),x(18),x(19),x(20),
      x(21),x(22),x(23),x(24),x(25),x(26),x(27),x(28), br.value._4, br.value._2, br.value._3));
    spark.createDataFrame(modified , schema);

  }
  def writeToFile(df : sql.DataFrame, fileLocation : String,br : Broadcast[(String, String, String, String)]) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fullPath = fileLocation+"/year=%s/month=%s/date=%s"
    val path = String.format(fileLocation, br.value._4, br.value._2, br.value._3)
    fs.delete(new Path(path), true)
    df.write.mode("append").partitionBy("year","month","date").parquet(fileLocation)


  }
  def main (args : Array[String]) : Unit = {
    val SDRFile = args(1);
    val date = args(0);
    val dateSplit = date.split("/");
    val year = dateSplit(2);
    val month = dateSplit(0);
    val day = dateSplit(1);
    val brDate = sc.broadcast(date, month, day, year);
    val source = readFile(SDRFile);
    println(source.count());
    val structuredDateFrame = structure(source, brDate);
    structuredDateFrame.show();
    writeToFile(structuredDateFrame,args(2) ,brDate);
  }

}