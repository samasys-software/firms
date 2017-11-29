import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
object Practice {
  def kathir(s :String, s1: String): String = s.concat(s1);
  def main(args:Array[String]): Unit ={
    val sc = SparkSession.builder().appName("Test").getOrCreate().sparkContext;
    val spark = SparkSession.builder().appName("Test").getOrCreate();
    val location = "C:\\Users\\kathi\\Desktop\\BIG_DATA\\AllStocks.csv";
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    val date = dateFormat.format(new Date());
    val dateSplit = date.split("/");
    val month = dateSplit(0);
    val day= dateSplit(1);
    val year = dateSplit(2);
    val br = sc.broadcast(date, month, day, year)
    val source = sc.textFile(location).map(_.split(","))
    val array = Array(StructField("Ticker", StringType, nullable = true), StructField("Date" , StringType, nullable = true), StructField("Price", DoubleType, nullable = true));
    val schema = StructType(array);
    import  spark.implicits._
    val modified = source.map(row=> Row(row(0), row(1), row(9).toDouble));
    val df = spark.createDataFrame(modified, schema);
    println(df.show());
    df.createOrReplaceTempView("all_quotes")
    val query = spark.sql("select Ticker, avg(Price) from all_quotes group by Ticker");
    query.map(row=>(row(0).toString, br.value._1 ,row(1).toString)).write.partitionBy("Ticker", year, month , day)
      .parquet("C:\\Users\\kathi\\Desktop\\BIG_DATA\\average_each_stock\\AverageByStock"+System.currentTimeMillis());





  }

}
