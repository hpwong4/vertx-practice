package com.hypo.practice.chapter02.context;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hbwang
 */
public class ContextUser {
  static Logger logger = LoggerFactory.getLogger(ContextUser.class);

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    vertx.getOrCreateContext().runOnContext(v -> logger.info("ABC"));
    vertx.getOrCreateContext().runOnContext(v -> logger.info("123"));

    Context context = vertx.getOrCreateContext();
    context.put("foo", "bar");

    context.exceptionHandler(throwable -> {
      if ("Tada".equals(throwable.getMessage())) {
        logger.info("Got a _Tada_ exception.");
      } else {
        logger.error("Woops", throwable);
      }
    });

    context.runOnContext(v -> {
      throw new RuntimeException("Tada");
    });

    context.runOnContext(v -> {
      logger.info("foo = {}", (String) context.get("foo"));
    });
  }
}
