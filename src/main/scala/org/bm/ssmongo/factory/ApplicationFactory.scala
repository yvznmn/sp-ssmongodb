package org.bm.ssmongo.factory

import org.apache.spark.sql.SparkSession
import org.bm.ssmongo.core.StreamingJob
import org.bm.ssmongo.entities.GlobalConfigEnt
import org.bm.ssmongo.job.StreamingFromKafkaToMongo

object ApplicationFactory {

  def get_streaming_application_instance(spark:SparkSession, params:GlobalConfigEnt): StreamingJob[GlobalConfigEnt] = {
    new StreamingFromKafkaToMongo(spark, params)
  }

}
