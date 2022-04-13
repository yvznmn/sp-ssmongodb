package org.bm.ssmongo.core

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger

import org.bm.ssmongo.entities._
import org.bm.ssmongo.utils.MongoUtils._
import org.bm.ssmongo.utils.ConfigUtils

//unit_test, dependency_injection, logging, schemas_in_mongo_as_json

object ss {

  print("Start\n")

  def read_stream_csv_file(spark:SparkSession, file_type: String, header_value: Boolean, table_schema:StructType, source_path:String): DataFrame = {
    val df = spark.readStream
      .format(file_type)
      .option("header", header_value)
      .schema(table_schema)
      .load(source_path)

    println(df.isStreaming)

    df

  }


  def write_row(message:DataFrame, id:Long): Unit = {
    message.persist()
    message.write
      .format("mongo")
      .mode("append")
      .save()
    message.persist()
  }



  def main(args: Array[String]): Unit = {

//    val env:String = ConfigUtils.env
    println(args(0))
    println(args(1))
    val env:String = args(0)
    val table_name:String = args(1)
    val connection_to_prop = ConfigUtils.read_environment_variables(env)
    val mongoURL_plain = connection_to_prop.getProperty("mongoURL_plain")
    val db_name = connection_to_prop.getProperty("db_name")
    val metadata_collection = connection_to_prop.getProperty("metadata_collection")
    val my_source_path = connection_to_prop.getProperty("my_source_path")
    val my_target_path = connection_to_prop.getProperty("my_target_path")
    val my_checkpoint_path = connection_to_prop.getProperty("my_checkpoint_location_value")
    val target_collection = table_name

    val db_conf_entity:Db_conf_entity = Db_conf_entity(mongoURL_plain, db_name, metadata_collection)
    val table_to_collection:Db_conf_entity = Db_conf_entity(mongoURL_plain, db_name, target_collection)


    val table_schema = read_spark_schema_from_mongo(table_name,db_conf_entity)
    val mongoURL = create_colection_URL(table_to_collection)
    print(mongoURL)


//    val table_schema = StructType(Array(
//      StructField("STATION",StringType,true),
//      StructField("STATION_NAME",StringType,true),
//      StructField("DATE",StringType,true),
//      StructField("HPCP",FloatType,true))
//    )
//    print(table_schema)

    val spark = SparkSession
      .builder()
      .master(connection_to_prop.getProperty("master"))
      .appName(connection_to_prop.getProperty("app_name"))
      .config(connection_to_prop.getProperty("mongo_config_input_uri"), mongoURL)
      .config(connection_to_prop.getProperty("mongo_config_output_uri"), mongoURL)
      .config(connection_to_prop.getProperty("spark_config_jars_packages_key"), connection_to_prop.getProperty("spark_config_jars_packages_value"))
      .getOrCreate()

    val readstream_df = read_stream_csv_file(
      spark,
      connection_to_prop.getProperty("spark_readstream_csv_format"),
      connection_to_prop.getProperty("spark_readstream_option_header_value").toBoolean,
      table_schema,
      my_source_path
    )

    readstream_df.writeStream
      .foreachBatch(write_row _)
      .trigger(Trigger.ProcessingTime(connection_to_prop.getProperty("trigger_processing_time")))
      .option(connection_to_prop.getProperty("my_checkpoint_location_key"), my_checkpoint_path)
      .start()
      .awaitTermination()


  }


}
