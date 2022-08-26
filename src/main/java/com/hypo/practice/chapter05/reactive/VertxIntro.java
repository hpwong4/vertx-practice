package com.hypo.practice.chapter05.reactive;


import io.reactivex.Completable;
import io.reactivex.Observable;
import io.vertx.core.Vertx;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.RxHelper;

import java.util.concurrent.TimeUnit;

public class VertxIntro extends AbstractVerticle {

  @Override
  public Completable rxStart() {
    Observable.interval(1, TimeUnit.SECONDS, RxHelper.scheduler(vertx)).subscribe(n -> System.out.println("tick"));

    return vertx.createHttpServer().requestHandler(r -> r.response().end("OK")).rxListen(8080).ignoreElement();
  }

  public static void main(String[] args) {
    Vertx.vertx().deployVerticle(new VertxIntro());
  }
}
