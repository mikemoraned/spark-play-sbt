package com.houseofmoran.spark.play.twitter

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import com.github.nscala_time.time.Imports._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.eclipse.jetty.server.{Server, Request}
import org.eclipse.jetty.server.handler.AbstractHandler
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class HelloWorld extends AbstractHandler {
  override def handle(target: String, baseRequest: Request,
                      request: HttpServletRequest, response: HttpServletResponse): Unit =
  {
    response.setContentType("text/html; charset=utf-8")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter().println("<h1>Hello World</h1>")
    baseRequest.setHandled(true);
  }
}

object TwitterVisApp {
  def main(args: Array[String]): Unit = {
    val server = new Server(8080)
    server.setHandler(new HelloWorld())
    server.start()
    server.join()
  }
}
