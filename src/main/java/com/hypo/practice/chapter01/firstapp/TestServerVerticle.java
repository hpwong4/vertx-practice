package com.hypo.practice.chapter01.firstapp;

import com.google.gson.Gson;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestServerVerticle extends AbstractVerticle {
  @Override
  public void start() throws Exception {
    // http server，监听 8080
    vertx.createHttpServer().requestHandler(httpServerRequest -> {
      httpServerRequest.bodyHandler(buffer -> {
        JsonObject data = buffer.toJsonObject();
//        System.out.println("request = " + data);
      });
      httpServerRequest.response().end(aaa());
    }).listen(9090);
  }


  private static String aaa() {
//    CtrResponse res = new CtrResponse();
//    res.code = 200;
//    res.result = new HashMap<>();
//    res.result.put(140, 0.3);
//    res.result.put(150, 0.2);

    List<List<Double>> c = new ArrayList<>();
    List<Double> ctrList = new ArrayList<>();
    ctrList.add(0.555);
    ctrList.add(0.44);
    c.add(ctrList);
    PredictResponse predictResponse = new PredictResponse();
    predictResponse.outputs = c;
    Gson gson = new Gson();
    return gson.toJson(predictResponse);
  }

  public static class CtrResponse {

    int code;
    HashMap<Integer, Double> result;

  }
  @Data
  static public class PredictResponse {

    private List<List<Double>> outputs;
    private String error;
  }

}
