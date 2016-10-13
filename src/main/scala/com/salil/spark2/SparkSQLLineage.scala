package com.salil.spark2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by salilsurendran on 9/20/16.
  */
object SparkLineage {

  def main(args: Array[String]) {
    val sparkSession = SparkSession
      .builder()
      .appName("Java Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate();
    // val hiveContext = new org.apache.spark.sql.hive.HiveContext(new SparkContext())
    if (sparkSession.sql("SHOW TABLES").collect().length == 0) {
      sparkSession.sql("CREATE TABLE sample_07 (code string,description string,total_emp int,salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")
      sparkSession.sql("LOAD DATA INPATH '/user/root/sample_07.csv' OVERWRITE INTO TABLE sample_07")
    }
    var df = sparkSession.sql(args(0))
    df = df.filter(df("sal") > 180000)
    df.write.saveAsTable("sample_07_150k_" + System.currentTimeMillis())
    df.write.parquet("/user/root/sample_07_150k_pq_" + System.currentTimeMillis())
    df.write.json("/user/root/sample_07_150k_json_" + System.currentTimeMillis())
    //df.write.text("/user/root/sample_07_150k_text_" + System.currentTimeMillis())
    println("toString : " + df.toString())
    println("explain : " )
    df.explain(true)
    for(i <- 0 until df.inputFiles.length){
      println("i'th element is: " + df.inputFiles(i));
    }
  }
}

object SQLSparkLineage {

  def main(args: Array[String]) {
    val sparkSession = SparkSession
      .builder()
      .appName("Java Spark Hive Example")
      //.master("local[4]")
      .enableHiveSupport()
      .getOrCreate();
    // val hiveContext = new org.apache.spark.sql.hive.HiveContext(new SparkContext())
    if (sparkSession.sql("SHOW TABLES").collect().length == 0) {
      sparkSession.sql("CREATE TABLE sample_07 (code string,description string,total_emp int,salary int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")
      sparkSession.sql("LOAD DATA INPATH '/user/root/sample_07.csv' OVERWRITE INTO TABLE sample_07")
    }
    var df = sparkSession.sql(args(0))
    df = df.filter(df("sal") > 180000)
    df.write.saveAsTable("sample_07_150k_" + System.currentTimeMillis())
    df.write.parquet("/user/root/sample_07_150k_pq_" + System.currentTimeMillis())
    df.write.json("/user/root/sample_07_150k_json_" + System.currentTimeMillis())
    //df.write.text("/user/root/sample_07_150k_text_" + System.currentTimeMillis())
    println("toString : " + df.toString())
    println("explain : " )
    df.explain(true)
    for(i <- 0 until df.inputFiles.length){
      println("i'th element is: " + df.inputFiles(i));
    }
    val l = List[LogicalPlan]()
    df.queryExecution.optimizedPlan.children.foldLeft(l)((acc, plan) => plan match {
      case Project(_,_) => plan::acc
      case _ => acc
    })
  }

  /*def myMethod[T >: LogicalPlan](l:List[T], df:DataFrame): Unit ={
    df.queryExecution.optimizedPlan.children.foldLeft(l)((acc, plan) => plan match {
      case Project => plan::acc
      case _ => acc
    })
  }*/
}

object SparkNavigatorLineage {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Lineage Application")
      .enableHiveSupport()
      .getOrCreate();

    val dfFromHive = spark.sql("from sample_07 select code,description,salary")
    dfFromHive.select("code", "description").write.saveAsTable("new_sample_07_" + System.currentTimeMillis())

    val dfCustomers = spark.read.load("/user/root/customers.parquet").select("id","name")
    dfCustomers.write.save("/user/root/abc_" + System.currentTimeMillis() + ".parquet")

    val rdd = spark.sparkContext.textFile("/user/root/people.csv")
    val outputRDD = rdd.map(_.split(",")).filter(p => p(1).length > 8).map(x => x(0) + ":" + x(1))
    outputRDD.saveAsTextFile("/user/root/output_" + System.currentTimeMillis())
    outputRDD.saveAsTextFile("s3://cloudera-dev-s3bugblitz/salil/people_" + System.currentTimeMillis())

    val globRdd = spark.sparkContext.textFile("/user/root/glob/*.txt")
    val counts = globRdd.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("/user/root/counts_" + System.currentTimeMillis())

    val wordCount = globRdd.flatMap(line => line.split(" ")).count
    val wordCountRDD = spark.sparkContext.parallelize(Seq(wordCount))
    wordCountRDD.saveAsTextFile("/user/root/res1")

    //This is a test
    val dfFromJson = spark.read.json("/user/root/json/people1.json",
      "/user/root/json/people2.json", "/user/root/json/people3.json")
      .select("name","age","phone","zip")
    dfFromJson.filter(dfFromJson("age") > 25).write.partitionBy("age","zip")
      .save("/user/root/partitioned_example_"+ System.currentTimeMillis())
  }
}

