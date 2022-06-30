package com.hypo.practice.chapter02.worker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerVerticle extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(WorkerVerticle.class);

  @Override
  public void start() throws Exception {
    vertx.setPeriodic(10_000, id -> {
      try {
        logger.info("Zzz...");
        Thread.sleep(8000);
        logger.info("Up!");
      } catch (InterruptedException e) {
        logger.error("Woops", e);
      }
    });
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions().setInstances(2).setWorker(true);
//    DeploymentOptions options = new DeploymentOptions().setInstances(2);
    vertx.deployVerticle("com.hypo.practice.chapter02.worker.WorkerVerticle", options);
  }
}
