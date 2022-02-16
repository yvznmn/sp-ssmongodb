package org.bm.ssmongo.entities

case class SimpleResultEntity(
                             _id:String,
                             info:String,
                             value:Int
                             )

case class MongoResultEntryEntity(
                                 entryId:String,
                                 value:String
                                 )


case class DeepMongoResultEntity(
                                  _id:String,
                                  info:MongoResultEntryEntity,
                                  value: Map[String, Int]
                                )
