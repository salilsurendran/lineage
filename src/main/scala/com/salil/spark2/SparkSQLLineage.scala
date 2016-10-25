package com.salil.spark2

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._


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
    println("explain : ")
    df.explain(true)
    for (i <- 0 until df.inputFiles.length) {
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

    sparkSession.listenerManager.register(new QueryExecutionListener {
      @DeveloperApi
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println("In Query ExecutionListener Failure")
      }

      @DeveloperApi
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println("In Query ExecutionListener Success : " + funcName)
        println("Optimized Plan String + " + qe.optimizedPlan.toString())
      }
    })
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
    println("explain : ")
    df.explain(true)
    for (i <- 0 until df.inputFiles.length) {
      println("i'th element is this: " + df.inputFiles(i));
    }
    val l = List[LogicalPlan]()
    df.queryExecution.optimizedPlan.productIterator.foldLeft(List[Any]())((acc, plan) => plan match {
      case Project(_, _) => plan :: acc
      case _ => {
        //System.out.println(plan)
        acc
      }
    })
  }

  Thread.sleep(100000)
  println("slept for 100 seconds")

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

    spark.sqlContext.listenerManager.register(new QueryExecutionListener {
      @DeveloperApi
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println("In SQLContext Query ExecutionListener Failure")
      }

      @DeveloperApi
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println("In SQLContext Query ExecutionListener Success")
        println("SQLContext  Optimized Plan String + " + qe.optimizedPlan.toString())
      }
    })

    spark.listenerManager.register(new QueryExecutionListener {
      @DeveloperApi
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println("In Query ExecutionListener Failure")
      }

      @DeveloperApi
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println("In Query ExecutionListener Success")
        println("Optimized Plan String + " + qe.optimizedPlan.toString())
      }
    })

    val dfFromHive = spark.sql("from sample_07 select code,description,salary")
    val dfFromHive2 =
      dfFromHive.select("code", "description").write.saveAsTable("new_sample_07_" + System.currentTimeMillis())

    val dfCustomers = spark.read.load("/user/root/customers.parquet").select("id", "name")
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
    wordCountRDD.saveAsTextFile("/user/root/wordcount_" + System.currentTimeMillis())

    //This is a test
    val dfFromJson = spark.read.json("/user/root/json/people1.json",
      "/user/root/json/people2.json", "/user/root/json/people3.json")
      .select("name", "age", "phone", "zip")
    dfFromJson.filter(dfFromJson("age") > 25).write.partitionBy("age", "zip")
      .save("/user/root/partitioned_example_" + System.currentTimeMillis())


    val rdd2 = spark.sparkContext.textFile("/user/root/people.csv")
    val schemaString = "first_name last_name code"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = rdd2.map(_.split(",")).map(p => Row(p(0), p(1), p(2)))
    val peopleDataFrame = spark.createDataFrame(rowRDD, schema)

    val df = spark.sql("select code,description,salary as sal from sample_07")
    val df2 = df.join(peopleDataFrame, df.col("code").equalTo(peopleDataFrame("code")))
    df2.take(2).foreach(println) //SQL gets called and QueryExecutionListener.onSuccess also gets called
    println("Count = " + df2.count) //SQL gets called and QueryExecutionListener.onSuccess also gets called
    df2.takeAsList(3).foreach(println) //SQL gets called and QueryExecutionListener.onSuccess also gets called
    val groupedDF = df2.groupBy(df2("first_name"))
    println(groupedDF.count())
    val groupedDF2 = groupedDF.mean()
    groupedDF2.show() //SQL gets called and QueryExecutionListener.onSuccess also gets called


    import spark.implicits._
    val jsonDF = spark.read.json("/user/root/arts.json") //job gets created
    val csvDS: Dataset[CSVStudent] = spark.sparkContext.textFile("/user/root/students.csv").map(l => l.split(","))
        .map(a => CSVStudent(a(0), a(1), a(2).toInt, a(3).toInt)).toDS()
    val csvDS2 = csvDS.join(jsonDF, jsonDF("Name") === csvDS("name")).select("age", "fees").filter("fees > 150")
    csvDS2.show() //SQL gets called and QueryExecutionListener.onSuccess also gets called
  }


  case class CSVStudent(name: String, subject: String, age: Int, marks: Int)

  case class JSONStudent(name: String, subject: String, fees: Int, marks: Int)

}

object LineageFailure {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Java Spark Hive Example")
      //.master("local[4]")
      //.enableHiveSupport()
      .getOrCreate();

    spark.listenerManager.register(new QueryExecutionListener {
      @DeveloperApi
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println("In Query ExecutionListener Failure")
      }

      @DeveloperApi
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println("In Query ExecutionListener Success")
        println("Optimized Plan String + " + qe.optimizedPlan.toString())
      }
    })

    spark.sqlContext.listenerManager.register(new QueryExecutionListener {
      @DeveloperApi
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println("In SQLContext Query ExecutionListener Failure")
      }

      @DeveloperApi
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println("In SQLContext Query ExecutionListener Success")
        println("SQLContext  Optimized Plan String + " + qe.optimizedPlan.toString())
      }
    })

    val dfCustomers = spark.read.load("/user/root/customers.parquet").select("id", "name")
    dfCustomers.take(2)
    dfCustomers.write.save("/user/root/abc_" + System.currentTimeMillis() + ".parquet")
  }
}

