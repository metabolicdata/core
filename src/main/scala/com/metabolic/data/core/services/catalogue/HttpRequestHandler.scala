package com.metabolic.data.core.services.catalogue

import org.apache.logging.log4j.scala.Logging
import scalaj.http.{Http, HttpResponse}

import scala.collection.mutable.ArrayBuffer

object HttpRequestHandler extends Logging {

  def sendHttpGetRequest(request: String, authToken: String): String = {

    logger.info(" Send Http Get Request (Start) ")

    try {

      val httpResponse: HttpResponse[String] = Http(request)
        .headers(Seq("Authorization" -> ("Bearer " + authToken), "Accept" -> "application/json"))
        .asString

      val response = if (httpResponse.code == 200) httpResponse.body
      else {
        logger.info("Bad HTTP response: code = " + httpResponse.code + httpResponse.body)
        return "ERROR"
      }

      logger.info(" Send Http Get Request (End) ")

      response

    } catch {
      case e: Exception => logger.info("Error in sending Get request: " + e.getMessage)
        "ERROR"
    }
  }

  def arrayBufferToJson(params: ArrayBuffer[(String, String)]): String = {

    var jsonString = "{"
    var count: Int = 0
    for (param <- params) {
      jsonString += "\"" + param._1 + "\":\"" + param._2 + "\"" + (if (count != params.length - 1) "," else "")
      count += 1
    }
    jsonString += "}"

    return jsonString

  }

  def sendHttpPostRequest(request: String, body: String, authToken: String): String = {

    logger.info(" Send Http Post Request (Start) ")

    try {
      val httpResponse: HttpResponse[String] = Http(request)
        .headers(Seq("Authorization" -> ("Bearer " + authToken), "Accept" -> "application/json"))
        .header("Content-Type", "application/json;charset=UTF-8")
        .header("Accept", "application/json")
        .postData(body.getBytes)
        .asString


      val response = if (httpResponse.code == 200) httpResponse.body
      else {
        logger.info("Bad HTTP response: code = " + httpResponse.code + httpResponse.body)
        "ERROR"
      }

      logger.info(s" Send Http Post Request (End) With Response: ${httpResponse.code + httpResponse.body}")

      response

    } catch {
      case e: Exception => logger.info("Error in sending Post request: " + e.getMessage)
        "ERROR"
    }
  }

}
