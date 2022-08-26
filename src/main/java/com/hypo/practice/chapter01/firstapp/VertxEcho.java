package com.hypo.practice.chapter01.firstapp;

import com.google.gson.Gson;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;

import java.util.HashMap;

public class VertxEcho {

  private static int numberOfConnections = 0;

  public static void main1(String[] args) {
    TestServerVerticle.CtrResponse res = new TestServerVerticle.CtrResponse();
    res.code = 200;
    res.result = new HashMap<>();
    res.result.put(140, 0.3);
    res.result.put(150, 0.2);

//    JsonObject object = Json.encode(res);

  }
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(TestServerVerticle.class.getName(), new DeploymentOptions().setInstances(8));
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
