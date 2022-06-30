package com.hypo.practice.chapter02.deploy;

import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deployer extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(Deployer.class);

  @Override
  public void start() throws Exception {
    long delay = 1000;
    for (int i=0; i<50; i++) {
      vertx.setTimer(delay, id -> deploy());
      delay = delay + 1000;
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
  }

  private void deploy() {
    vertx.deployVerticle(new EmptyVerticle(), ar -> {
      if (ar.succeeded()) {
        String id = ar.result();
        logger.debug("succ deploy id = {}", id);
        vertx.setTimer(5000, tid -> undeployLater(id));
      } else {
        logger.debug("fail when deploy. cause = ", ar.cause());
      }
    });
  }

  private void undeployLater(String id) {
    vertx.undeploy(id, ar -> {
      if (ar.succeeded()) {
        logger.debug("{} was undeployed", id);
      } else {
        logger.debug("{} could not be undeployed.", id);
      }
    });
  }
}
