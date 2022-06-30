package com.hypo.practice.chapter02.opts;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleVerticle extends AbstractVerticle {

  Logger logger = LoggerFactory.getLogger(SampleVerticle.class);

  @Override
  public void start() throws Exception {
    logger.info("n = {}", config().getInteger("n", -1));
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    for (int n=0; n<4; n++) {
      JsonObject conf = new JsonObject().put("n", n);
      DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(conf).setInstances(4);

      vertx.deployVerticle("com.hypo.practice.chapter02.opts.SampleVerticle", deploymentOptions);
    }
  }

}
