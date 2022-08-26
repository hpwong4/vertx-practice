package com.hypo.practice.test.verticles;

import com.google.gson.Gson;
import com.hypo.practice.test.utils.OptionUtil;
import com.hypo.practice.test.vo.CtrRequest;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(ServerVerticle.class);
  private static final String CONTENT_TYPE = "application/json";
  private static final Gson GSON = new Gson();

  private RedisAPI redisAPI;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // 1. 初始化 redis
    RedisOptions redisOptions = OptionUtil.getRedisOptions(config());
    if (redisOptions == null) {
      startPromise.fail("Failed get redis options.");
      return;
    }

    Redis redis = Redis.createClient(vertx, redisOptions);
    redis.connect()
        .onSuccess(conn -> {
          logger.info("Redis 连接成功..");
        })
        .onFailure(throwable -> {
          logger.error("Redis 连接异常. e = ", throwable.getCause());
          startPromise.fail(throwable);
        });

    redisAPI = RedisAPI.api(redis);


    // 2. http server
    // HttpServer 设置, KeepAlive、NoDelay
    HttpServerOptions httpServerOptions = new HttpServerOptions().setTcpKeepAlive(true).setTcpNoDelay(true);
    int port = config().getInteger("port", 8080);

    Router router = Router.router(vertx);

    router.route().consumes(CONTENT_TYPE);
    router.route().produces(CONTENT_TYPE);
    router.route().handler(BodyHandler.create());

    router.post().handler(this::route);

    // 通过 Router 创建 HttpServer
    vertx.createHttpServer(httpServerOptions)
        .requestHandler(router)
        .listen(port, ar -> {
          if (ar.succeeded()) {
            logger.info("Hermes Server started on 0.0.0.0:{}", port);
          } else {
            logger.error("Failed start Server on port({}), please check it. e = ", port, ar.cause());
            startPromise.fail(ar.cause());
          }
        });
  }

  private void route(RoutingContext context) {
    String body = context.body().asString();

    CtrRequest request = GSON.fromJson(body, CtrRequest.class);
//    CtrStrategy ctrStrategy = getCtrStrategy(ctrRequest.getGroupId());


  }


  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    super.stop(stopPromise);
  }
}
