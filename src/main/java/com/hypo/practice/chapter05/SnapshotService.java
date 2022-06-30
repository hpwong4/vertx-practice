package com.hypo.practice.chapter05;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotService extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(SnapshotService.class);

  @Override
  public void start() throws Exception {
    vertx.createHttpServer().requestHandler(request -> {
      if (badRequest(request)) {
        request.response().setStatusCode(400).end();
      }
      request.bodyHandler(buffer -> {
        logger.info("Latest temperature : {}", buffer.toJsonObject().encodePrettily());
        request.response().end();
      });
    }).listen(4000);
  }

  private boolean badRequest(HttpServerRequest request) {
    return !request.method().equals(HttpMethod.POST) || !"application/json".equals(request.getHeader("Content-Type"));
  }
}
