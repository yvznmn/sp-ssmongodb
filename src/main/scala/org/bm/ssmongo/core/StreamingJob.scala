package org.bm.ssmongo.core

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bm.ssmongo.entities.GlobalConfigEnt
import org.bm.ssmongo.utils.Logger

abstract class StreamingFromKafkaToMongo(spark: SparkSession, parameters:GlobalConfigEnt) extends Logger {

  final def run_streaming(): Unit = {

    try {
      this.setup_job()
      this.logger.info("Streaming initialized, next step is read input df.")

      val input_df = setup_input_stream(parameters)

      input_df match {
          case None => {
            this.logger.error("Input df is None, Houston, we have a problem here.")
            throw new RuntimeException("Input stream is None")
          }
          case Some(df) => {
            this.logger.info("Input data is ready, next step is write the data")
            this.write_df(Some(df))
            this.logger.info("Data is written, next step is terminate the streaming")
            this.stop_streaming()
            this.logger.info("Job is completed, teminating")
          }
        }
    }
    catch {
        case ex: Exception => throw ex

    }
    finally {
      finalize_the_job()
    }
  }

  protected def setup_job(): Unit

  protected def setup_input_stream(parameters:GlobalConfigEnt):Option[DataFrame]

  protected def write_df(dataframe:Option[DataFrame]):Unit

  protected def stop_streaming():Unit

  protected def finalize_the_job():Unit

}

