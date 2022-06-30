package com.hypo.practice.chapter02.deploy;

import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmptyVerticle extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(EmptyVerticle.class);

  @Override
  public void start() throws Exception {
    logger.debug("start EmptyVerticle");
  }

  @Override
  public void stop() throws Exception {
    logger.debug("stop EmptyVerticle");
  }
}
