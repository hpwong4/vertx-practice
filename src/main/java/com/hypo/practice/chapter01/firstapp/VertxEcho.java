package com.hypo.practice.chapter01.firstapp;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetSocket;

public class VertxEcho {

  private static int numberOfConnections = 0;

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    // tcp 服务，监听 3000
    vertx.createNetServer().connectHandler(VertxEcho::handleNewClient).listen(3000);

    // 定时任务
    vertx.setPeriodic(5000, id -> System.out.println(howMany()));

    // http server，监听 8080
    vertx.createHttpServer().requestHandler(httpServerRequest -> httpServerRequest.response().end(howMany())).listen(8080);
  }

  private static void handleNewClient(NetSocket socket) {
    numberOfConnections++;
    socket.handler(buffer -> {
      System.out.println("tcp request : " + buffer.toString());

      socket.write("hello " + buffer);
      if (buffer.toString().endsWith("/quit\n")) {
        socket.close();
      }
    });
    socket.closeHandler(v -> numberOfConnections--);
  }

  private static String howMany() {
    return "connections number : " + numberOfConnections;
  }
}
