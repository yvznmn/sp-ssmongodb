package org.bm.ssmongo.utils

trait Logger {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger("ApplicationLogger")

}
