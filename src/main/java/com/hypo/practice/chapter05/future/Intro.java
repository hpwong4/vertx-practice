package com.hypo.practice.chapter05.future;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Intro {

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    Promise<String> promise = Promise.promise();
    System.out.println("Waiting。。。");

    vertx.setTimer(5000, id -> {
      if (System.currentTimeMillis() % 2L == 0L) {
        promise.complete("OK");
      } else {
        promise.fail(new RuntimeException("Bad luck、、、"));
      }
    });


    Future<String> future = promise.future();
    future.onSuccess(System.out::println).onFailure(er -> System.out.println(er.getMessage()));

    promise.future().recover(err -> Future.succeededFuture("Lets say ok.")).map(String::toUpperCase)
        .flatMap(str -> {
          Promise<String> next = Promise.promise();
          vertx.setTimer(3000, id -> next.complete(">>> " + str));
          return next.future();
        }).onSuccess(System.out::println);


    CompletionStage<String> cs = promise.future().toCompletionStage();
    cs.thenApply(String::toUpperCase).thenApply(str -> "~~~" + str)
        .whenComplete((str, err) -> {
          if (err == null) {
            System.out.println(str);
          } else {
            System.out.println("Oh  " + err.getMessage());
          }
        });

    CompletableFuture<String> cf = CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return "5 second have elapsed";
    });

    Future.fromCompletionStage(cf, vertx.getOrCreateContext()).onSuccess(System.out::println).onFailure(Throwable::printStackTrace);
  }
}
