package com.metabolic.data.core.domain

case class DDBBConnection(engine: Option[String], host: Option[String], masterarn: Option[String],
                          password: Option[String], port: Option[String], read_replica_host: Option[String],
                          username: Option[String], dbname: Option[String])
