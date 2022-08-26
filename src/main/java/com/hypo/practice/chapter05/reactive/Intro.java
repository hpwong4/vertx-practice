package com.hypo.practice.chapter05.reactive;

import io.reactivex.Observable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Intro {

  public static void main(String[] args) throws InterruptedException {
    Observable.just(1, 2, 3).map(Objects::toString).map(s -> "@" + s).subscribe(System.out::println);

    Observable.<String>error(() -> new RuntimeException("woops"))
        .map(String::toUpperCase)
        .subscribe(System.out::println, Throwable::printStackTrace);

    Observable.just("--", "this", "is", "--", "a", "sequence", "of", "items", "!")
        .doOnSubscribe(d -> {
          System.out.println("Subscribed!");
        })
        .delay(5, TimeUnit.SECONDS)
        .filter(s -> !s.startsWith("--"))
        .doOnNext(x -> System.out.println("doOnNext: " + x))
        .map(String::toUpperCase)
        .buffer(3)
        .subscribe(
            pair -> System.out.println("next: " + pair),
            Throwable::printStackTrace,
            () -> System.out.println(">>>Done")
        );

    Thread.sleep(10_000);
  }
}
