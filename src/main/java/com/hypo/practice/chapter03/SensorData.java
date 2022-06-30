package com.hypo.practice.chapter03;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.stream.Collectors;

public class SensorData extends AbstractVerticle {

  final HashMap<String, Double> lastValues = new HashMap<>();

  @Override
  public void start() throws Exception {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer("sensor.updates", this::update);
    eventBus.consumer("sensor.average", this::average);
  }

  private void update(Message<JsonObject> message) {
    JsonObject body = message.body();
    lastValues.put(body.getString("id"), body.getDouble("temp"));
  }

  private void average(Message<JsonObject> message) {
    double avg = lastValues.values().stream().collect(Collectors.averagingDouble(Double::doubleValue));
    JsonObject json = new JsonObject().put("average", avg);

    message.reply(json);
  }
}
