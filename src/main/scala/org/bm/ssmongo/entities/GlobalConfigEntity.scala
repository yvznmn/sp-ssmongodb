package org.bm.ssmongo.entities

case class Db_conf_entity(
                    mongo_url: String,
                    database: String,
                    collection: String
                  )

case class GlobalConfigEntity()
