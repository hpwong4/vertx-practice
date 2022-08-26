package com.hypo.practice.chapter06;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

public class ProxyClient extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    SensorDataService service = SensorDataService.createProxy(vertx, "sensor.data-service");

    vertx.setPeriodic(5000, id -> {
      service.average(ar -> {
        if (ar.succeeded()) {
          System.out.println("avg = " + ar.result());
        } else {
          ar.cause().printStackTrace();
        }
      });
    });

  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("com.hypo.practice.chapter06.HeatSensor",
        new DeploymentOptions().setInstances(4));
    vertx.deployVerticle(new DataVerticle());
    vertx.deployVerticle(new ProxyClient());
  }
}
