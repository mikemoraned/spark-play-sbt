package com.houseofmoran.spark.play.twitter

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

import com.github.nscala_time.time.Imports._
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.eclipse.jetty.server.{Handler, Server, Request}
import org.eclipse.jetty.server.handler.{HandlerList, ResourceHandler, AbstractHandler}
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

    val resources = new ResourceHandler()
    resources.setWelcomeFiles(Array[String]("index.html"))
    resources.setResourceBase("./src/main/resources")

    val handlers = new HandlerList()
    handlers.setHandlers(Array[Handler]( resources, new HelloWorld() ))

    server.setHandler(handlers)
    server.start()
    server.join()
  }
}
