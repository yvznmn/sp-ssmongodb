package org.bm.ssmongo.job

import org.apache.arrow.util.VisibleForTesting
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bm.ssmongo.core.StreamingJob
import org.bm.ssmongo.entities.GlobalConfigEnt

object StreamingFromKafkaToMongo {

  var logger = Logger.getLogger(this.getClass)

}

class StreamingFromKafkaToMongo(spark:SparkSession, params:GlobalConfigEnt) extends StreamingJob[GlobalConfigEnt](spark, params) {

  logger.info("Starting the Streaming FLow from Kafka to MongoDB")

  override protected def setup_job(): Unit = {
    this.logger.info("Setting Up the job: setup_job()")
    print("setup_job\n")
  }

  @VisibleForTesting
  override protected[job] def setup_input_stream(parameters: GlobalConfigEnt): Option[DataFrame] = {
    this.logger.info("Setting Up the Input Streaming: setup_input_stream()")
    print("setup_input_stream\n")

    val columns = Seq("emp_id", "emp_name")
    val data = Seq(("1234", "Mike"),("3456", "Josh"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data))

    Some(df)

  }

  override protected[job] def write_df(dataframe: Option[DataFrame]): Unit = {
    this.logger.info("Writing the Streaming: write_df()")
    print("write_df\n")
  }

  override protected[job] def stop_streaming(): Unit = {
    this.logger.info("Stop the Streaming: stop_streaming() = spark stop")
    print("stop_streaming\n")
  }

  override protected[job] def finalize_the_job(): Unit = {
    this.logger.info("Finalize Job: finalize_the_job() = await termination")
    print("finalize_the_job\n")
  }

}


