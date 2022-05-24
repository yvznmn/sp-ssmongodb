package org.bm.ssmongo.entities

case class KafkaSpecificConfigurationEnt(
                                        security_protocol:String,
                                        ssl_truststore_location:String,
                                        starting_offsets:String,
                                        cache:String,
                                        debug:String,
                                        topic_name:String
                                        )

case class StreamingConfigurationEnt(
                                    kafka_bootstrap_servers:String,
                                    acks:String,
                                    kafka_offset_location:String,
                                    streaming_checkpoint:String,
                                    max_offsets_per_trigger:String,
                                    trigger_window:String,
                                    write_concurrency:String,
                                    rewindinding_hours:String,
                                    kafka_specific_configurations:KafkaSpecificConfigurationEnt
                                    )

case class Notifications(
                          host: String,
                          port: String,
                          to_address: String,
                          username: String,
                          passowrd: String,
                          trans_protocol: String,
                          environment: String
                        )
case class ControlTableConfig(
                               control_table_name: String,
                               control_table_schema_owner: String
                             )
case class ApplicationDetails(
                                application_name: String,
                                metalocation: String
                              )

case class GlobalConfigEnt(
                          streaming_configuration:StreamingConfigurationEnt,
                          notifications: Notifications,
                          control_table_config: ControlTableConfig,
                          application_details:ApplicationDetails
                          )
