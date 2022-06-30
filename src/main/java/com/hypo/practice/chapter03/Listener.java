package com.hypo.practice.chapter03;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;

public class Listener extends AbstractVerticle {
  Logger logger = LoggerFactory.getLogger(Listener.class);
  private final DecimalFormat format = new DecimalFormat("#.##");

  @Override
  public void start() throws Exception {
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer("sensor.updates", msg -> {
      JsonObject body = (JsonObject) msg.body();
      String id = body.getString("id");
      String temperature = format.format(body.getDouble("temp"));

      logger.info("{} reports a temperature ~ {}C", id, temperature);
    });
  }
}
