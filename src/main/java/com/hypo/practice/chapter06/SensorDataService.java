package com.hypo.practice.chapter06;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

// 次注解用来标记 event-bus 服务接口，以生成代理代码
@ProxyGen
@VertxGen
public interface SensorDataService {

  /**
   * 实例工厂
   * @param vertx vertx
   * @return service
   */
  static SensorDataService create(Vertx vertx) {
    return new SensorDataServiceImpl(vertx);
  }


  static SensorDataService createProxy(Vertx vertx, String address) {
    return new SensorDataServiceVertxEBProxy(vertx, address);
  }

  void valueFor(String sensorId, Handler<AsyncResult<JsonObject>> handler);
  void average(Handler<AsyncResult<JsonObject>> handler);
}
