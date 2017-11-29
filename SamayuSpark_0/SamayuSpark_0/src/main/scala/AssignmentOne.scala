

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object AssignmentOne {
  val sc = SparkSession.builder().appName("Test").master("local").getOrCreate().sparkContext;
  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate();
  def readFile(s: String): RDD[Array[String]] = {
    sc.textFile(s).map(_.split(","))
  }

  def dataFrame (rdd : RDD[Array[String]]) = {
    val array = Array(StructField("Ticker", StringType, nullable = true), StructField("Date" , StringType, nullable = true), StructField("Price", DoubleType, nullable = true));
    val schema = StructType(array);
    val modified = rdd.map(y=> Row(y(0),y(1),y(9).toDouble));
    spark.createDataFrame(modified, schema);

  }


  def main (args: Array[String]): Unit ={
    import spark.implicits._
    val location = "C:\\Users\\kathi\\Desktop\\BIG_DATA\\AllStocks.csv";
    val file = readFile(location);
    val df = dataFrame(file);
    df.createOrReplaceTempView("all_quotes")
    val query = spark.sql("select Ticker, avg(Price) from all_quotes group by Ticker");
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    val date = dateFormat.format(new Date());
    val dateSplit = date.split("/");
    val month = dateSplit(0);
    val day= dateSplit(1);
    val year = dateSplit(2);
    val br = sc.broadcast(date, month, day, year)

    query.map(row=>(row(0).toString, br.value._1 ,row(1).toString)).write.partitionBy("Ticker")
      .parquet("C:\\Users\\kathi\\Desktop\\BIG_DATA\\average_each_stock\\AverageByStock"+System.currentTimeMillis());

}

}
