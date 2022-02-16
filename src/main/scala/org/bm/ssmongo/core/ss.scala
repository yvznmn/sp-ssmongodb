package org.bm.ssmongo.core

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger

//unit_test, dependency_injection, logging, schemas_in_mongo_as_json

object ss {

  print("Start")

//  val client = MongoClient()
//  val db:MongoDatabase = client.getDatabase("my_db")
//  db.createCollection("my_col")
//  val my_col = db.getCollection("my_col")

  val mongoURL = "mongodb://127.0.0.1/ssmongo.ssmongocol"
  val my_source_path = "src/main/scala/org/bm/ssmongo/testData"
  val my_target_path = "src/main/scala/org/bm/ssmongo/target"
  val my_checkpoint_path = "src/main/scala/org/bm/ssmongo/target/checkpoint"

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SSMongo")
    .config("spark.mongodb.input.uri", mongoURL)
    .config("spark.mongodb.output.uri", mongoURL)
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
    .getOrCreate()

  import spark.implicits._

  val my_schema = StructType(Array(
    StructField("STATION", StringType, true),
    StructField("STATION_NAME", StringType, true),
    StructField("DATE", StringType, true),
    StructField("HPCP", FloatType, true)
  ))

  var df = spark.readStream
    .format("csv")
    .option("header", true)
//    .option("spark.executor.processTreeMetrics.enabled", false)
    .schema(my_schema)
//    .option("checkpointLocation", "/tmp/my_load/")
    .load(my_source_path)
//    .as[my_schema_class]


  println(df.isStreaming)

  def my_save(message:DataFrame, id:Long): Unit = {
    message.persist()
    message.write
      .format("mongo")
      .mode("append")
//      .option("database", "db_name")
//      .option("collection", "collection_name")
      .save()
    message.persist()
  }



//    .option("checkpointLocation","src/main/scala/org/bm/ssmongo/target")
//    .format("console")
//    .format("mongo")
//    .option("database", "ssmongo")
//    .option("collection", "ssmongocol")
//    .trigger(Trigger.ProcessingTime("2 seconds"))







//  def write_mongo_row(batch_df:DataFrame,batch_id:Int): Unit = {
//    val mongoURL = "mongodb://localhost:27017/my_db.my_col"
//    batch_df.persist()
//    batch_df.write
//      .format("mongo")
//      .mode("append")
//      .option("uri", mongoURL)
//      .save()
//    batch_df.unpersist()
//    ()
//  }






//  val doc: Document = Document("_id" -> 0, "name" -> "MongoDB", "type" -> "database",
//    "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
//
//  my_col.insertOne(doc)
//
//  println(my_col.find().first())








  def main(args: Array[String]): Unit = {

    println("main")
    df.writeStream.foreachBatch(my_save _).trigger(Trigger.ProcessingTime("2 seconds")).option("checkpointLocation", my_checkpoint_path).start().awaitTermination()
    //only command line args


  }


}
