

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import sys.process.stringSeqToProcess
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._

object IntSeries
{
  def main(args: Array[String]) 
  {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]").set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf)
    val sql = new org.apache.spark.sql.SQLContext(sc)
    import sql.implicits._

    val intRange = List.range(1, 101)
    val df = sc.parallelize(intRange).toDF
    df.take(100).foreach(println)
    df.write.format("parquet").save("Int_Series")
  }
}