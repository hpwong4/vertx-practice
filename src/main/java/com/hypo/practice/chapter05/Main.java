package com.hypo.practice.chapter05;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Main {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("com.hypo.practice.chapter05.HeatSensor",
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", 3000)));
    vertx.deployVerticle("com.hypo.practice.chapter05.HeatSensor",
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", 3001)));
    vertx.deployVerticle("com.hypo.practice.chapter05.HeatSensor",
        new DeploymentOptions().setConfig(new JsonObject().put("http.port", 3002)));

    vertx.deployVerticle(new SnapshotService());
    vertx.deployVerticle(new CollectorService());
  }
}
