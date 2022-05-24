package org.bm.ssmongo.job

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.bm.ssmongo.entities.{ApplicationDetails, ControlTableConfig, GlobalConfigEnt, KafkaSpecificConfigurationEnt, Notifications, StreamingConfigurationEnt}
import org.bm.ssmongo.factory.ApplicationFactory
import org.bm.ssmongo.utils.ConfigUtils

object ApplicationLibrary {

  val logger = Logger.getLogger(this.getClass)

  def main(args:Array[String]): Unit = {

    val env:String = "PROD"
    val connection_to_prop = ConfigUtils.read_environment_variables(env)
    val mongoURL = connection_to_prop.getProperty("mongoURL_plain")


    val spark = SparkSession
      .builder()
      .master(connection_to_prop.getProperty("master"))
      .appName(connection_to_prop.getProperty("app_name"))
      .config(connection_to_prop.getProperty("mongo_config_input_uri"), mongoURL)
      .config(connection_to_prop.getProperty("mongo_config_output_uri"), mongoURL)
      .config(connection_to_prop.getProperty("spark_config_jars_packages_key"), connection_to_prop.getProperty("spark_config_jars_packages_value"))
      .getOrCreate()

    val arg_1:KafkaSpecificConfigurationEnt = KafkaSpecificConfigurationEnt(
      security_protocol = "ssl",
      ssl_truststore_location = "dunno",
      starting_offsets = "5",
      cache = "yes",
      debug = "no",
      topic_name = "my_topic"
    )

    val arg_2:StreamingConfigurationEnt = StreamingConfigurationEnt(
      kafka_bootstrap_servers = "blahblah",
      acks = "dunno",
      kafka_offset_location = "5",
      streaming_checkpoint = "abc",
      max_offsets_per_trigger = "10",
      trigger_window = "10",
      write_concurrency = "17",
      rewindinding_hours = "13",
      kafka_specific_configurations = arg_1
    )

    val arg_3:Notifications = Notifications(
      host = "love",
      port = "1111",
      to_address = "asdfa",
      username = "numan",
      passowrd = "kar",
      trans_protocol = "yes",
      environment = "yes"
    )

    val arg_4: ControlTableConfig = ControlTableConfig(
      control_table_name = "table",
      control_table_schema_owner = "owner"
    )

    val arg_5:ApplicationDetails = ApplicationDetails(
      application_name = "my_name",
      metalocation = "loc"
    )

    val my_args:GlobalConfigEnt = GlobalConfigEnt(
      streaming_configuration = arg_2,
      notifications = arg_3,
      control_table_config = arg_4,
      application_details = arg_5
    )

    ApplicationFactory.get_streaming_application_instance(spark, my_args).run_streaming()
  }



}
