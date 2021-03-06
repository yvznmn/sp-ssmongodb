package org.bm.ssmongo.utils

import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import org.bm.ssmongo.core.ss.getClass
import org.apache.log4j.Logger

object ConfigUtils {

//  var env:String = "PROD"
  val logger:Logger = Logger.getLogger(this.getClass)

  def read_environment_variables(env:String): Properties = {

    val connection_to_prop = new Properties()

    try {
      connection_to_prop.load(getClass().getResourceAsStream(s"/$env/env.properties"))
      PropertyConfigurator.configure(connection_to_prop)
    } catch {
      case ex: Exception => println(ex, "\nProperties File Not Read!\n")
    }

    connection_to_prop
  }

}
