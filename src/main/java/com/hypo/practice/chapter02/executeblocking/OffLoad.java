package com.hypo.practice.chapter02.executeblocking;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffLoad extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(OffLoad.class);

  @Override
  public void start() throws Exception {
    vertx.setPeriodic(5000, id -> {
      logger.info("Tick");
      vertx.executeBlocking(this::blockingCode, this::resultHandler);
    });
  }

  private void resultHandler(AsyncResult<String> ar) {
    if (ar.succeeded()) {
      logger.info("blocking code result : {}", ar.result());
    } else {
      logger.error("Woops", ar.cause());
    }
  }

  private void blockingCode(Promise<String> promise) {
    logger.info("Blocking code running");

    try {
      Thread.sleep(4000);
      logger.info("Done!");
      promise.complete("OK");
    } catch (InterruptedException e) {
      promise.fail(e);
    }
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new OffLoad());
  }
}
