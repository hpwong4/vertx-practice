package com.hypo.practice.chapter06;

import com.hypo.practice.chapter06.reactivex.SensorDataService;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.RxHelper;

import java.util.concurrent.TimeUnit;

public class RxProxyClient extends AbstractVerticle {

  @Override
  public void start() throws Exception {
    SensorDataService service = SensorDataService.createProxy(vertx, "sensor.data-service");
    service
        .rxAverage()
        .delaySubscription(3, TimeUnit.SECONDS, RxHelper.scheduler(vertx))
        .repeat()
        .map(data -> "avg = " + data.getDouble("average"))
        .subscribe(System.out::println);
  }

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle("com.hypo.practice.chapter06.HeatSensor",
        new DeploymentOptions().setInstances(4));
    vertx.deployVerticle(new DataVerticle());
    vertx.deployVerticle(new RxProxyClient());
  }
}
