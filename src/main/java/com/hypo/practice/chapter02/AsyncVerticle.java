package com.hypo.practice.chapter02;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

/**
 * @author hbwang
 */
public class AsyncVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.createHttpServer().requestHandler(request -> {
      request.response().end("OK");
    }).listen(8080, result -> {
      if (result.succeeded()) {
        startPromise.complete();
      } else {
        startPromise.fail(result.cause());
      }
    });
  }
}
