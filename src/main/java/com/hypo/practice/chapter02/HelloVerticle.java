package com.hypo.practice.chapter02;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelloVerticle extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(HelloVerticle.class);

  private long counter = 1;

  @Override
  public void start() throws Exception {

    vertx.setPeriodic(5000, id -> {
      logger.debug("tick");
    });

    vertx.createHttpServer().requestHandler(req -> {
      logger.debug("req from {}, counter = {}", req.remoteAddress().host(), counter++);
      req.response().end("Hello");
    }).listen(8080);

    logger.debug("open http://localhost:8080");
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new HelloVerticle());
  }
}
