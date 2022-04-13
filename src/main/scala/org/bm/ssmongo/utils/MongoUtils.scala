package org.bm.ssmongo.utils

import com.mongodb.client.model.UpdateOptions
import org.apache.log4j.PropertyConfigurator
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, Observer, SingleObservable}
import org.bson.BsonDocument
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.model.Filters._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parseDataType
import Helpers._
import org.apache.spark.sql.types.{StructField, StructType}
import org.bm.ssmongo.entities._

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.bm.ssmongo.utils.ConfigUtils._


object MongoUtils {

  def create_colection_URL(mongo_url_entity: Db_conf_entity): String = {
    s"${mongo_url_entity.mongo_url}${mongo_url_entity.database}.${mongo_url_entity.collection}"
  }

  def client_open(mongoURL:String): MongoClient = {
    val client: MongoClient = MongoClient(mongoURL)
    client
  }

  def connect_mongo_collection(db_conf: Db_conf_entity, client: MongoClient): MongoCollection[Document] = {

    val db: MongoDatabase = client.getDatabase(db_conf.database)
    val col: MongoCollection[Document] = db.getCollection(db_conf.collection)

    logger.info(s"Collection Name is = $col")
    col

  }


  def read_spark_schema_from_mongo(table_name:String, db_conf: Db_conf_entity): StructType = {

    logger.info("read_spark_schema_from_mongo function is started.\n")
    val client = client_open(db_conf.mongo_url)
    val col:MongoCollection[Document] = connect_mongo_collection(db_conf, client)
    var outputSchema: ListBuffer[StructField] = ListBuffer[StructField]()

    try {
      val json_schema = col.find(equal("table_name", table_name))

      json_schema.subscribe( new Observer[Document] {
        override def onNext(result: Document): Unit = logger.info(result.toJson()+"\n")
        override def onError(e: Throwable): Unit = logger.info("Failed\n" + e.getMessage)
        override def onComplete(): Unit = logger.info("Completed\n")
      })
      Thread.sleep(3000)

      val res = json_schema.headResult()

      res.get("columns").foreach(x => {
        x.asArray().getValues.forEach(y => {
          outputSchema += StructField(
            y.asDocument().getString("name").getValue,
            parseDataType(y.asDocument().getString("type").getValue),
            y.asDocument().getBoolean("nullable").getValue
          )
        }
        )
      })

      client.close()

      logger.info(outputSchema+"\n")
    } catch {
      case ex: Exception => logger.error(ex+ "Spark Schema Not Read from MongoDB!\n")
    }

    StructType(outputSchema)

  }

  def insert_json_schema_to_mongo(path:String, db_conf_entity: Db_conf_entity): Unit = {

    logger.info("insert_json_schema_to_mongo function is started! \n")
    var schema_txt = ""
    val client = client_open(db_conf_entity.mongo_url)

    try {

      val bufferedSource = Source.fromFile(path, enc = "UTF-8")
      for (line <- bufferedSource.getLines) {
        schema_txt = schema_txt + line
      }
      schema_txt = schema_txt.replaceAll("\\s", "")
      bufferedSource.close
    } catch {
    case ex: Exception => logger.error(ex+ "\n Schema text file not read!\n")
  }
    val schema_bson = BsonDocument.parse(schema_txt)

//    val schema_json = json.JsonParser.apply(schema_txt)


      try {

        val col = connect_mongo_collection(db_conf_entity, client)

        val filterDoc = schema_bson
        val update_document = Document("$set" -> Document(schema_txt))
        val update_options = (new UpdateOptions()).upsert(true)

        val res:SingleObservable[UpdateResult] = col.updateOne(filterDoc, update_document, update_options)


        res.subscribe(new Observer[UpdateResult] {
          override def onNext(result: UpdateResult): Unit = logger.info(s"onNext: $result")
          override def onError(e: Throwable): Unit = logger.info(s"onError: $e")
          override def onComplete(): Unit = logger.info("Schema ingestion is onComplete")
        })
        Thread.sleep(3000)

        client.close()




//        print("After insertion\n")
//        val id = col.find(schema_bson)
//
//        id.subscribe( new Observer[Document] {
//          override def onNext(result: Document): Unit = println(result.toJson())
//          override def onError(e: Throwable): Unit = println("Failed" + e.getMessage)
//          override def onComplete(): Unit = println("Completed")
//        })
//        Thread.sleep(3000)

      } catch {
        case ex: Exception => println(ex)
      }
  }

  def main(args: Array[String]): Unit = {

    val connection_to_prop = ConfigUtils.read_environment_variables(ConfigUtils.env)

    val mongoURL = connection_to_prop.getProperty("mongoURL_plain")
    val metadata_db = connection_to_prop.getProperty("db_name")
    val metadata_collection = connection_to_prop.getProperty("metadata_collection")
    val json_loc = connection_to_prop.getProperty("schema_json_location")
    val my_db:Db_conf_entity = Db_conf_entity(mongoURL, metadata_db, metadata_collection)

//    insert_json_schema_to_mongo(json_loc, my_db)
//    print(read_spark_schema_from_mongo("STATION_HPCP", my_db))



  }

}
