package com.salil.spark2

import org.apache.spark.sql.SparkSession
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
  }
}

