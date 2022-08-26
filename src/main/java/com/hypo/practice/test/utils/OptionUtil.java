package com.hypo.practice.test.utils;

import com.google.common.base.Strings;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;

/**
 * 配置文件提取
 *
 * @author hbwang
 */
public class OptionUtil {

  /**
   * 获取 vertx redis options
   *
   * @param config config
   * @return redisOptions
   */
  public static RedisOptions getRedisOptions(JsonObject config) {
    String redisUri = config.getString("lettuce.uri", "redis://172.31.236.163:6380,redis://172.31.236.163:6381,redis://172.31.236.163:6382");
    if (Strings.isNullOrEmpty(redisUri)) {
      return null;
    }

    // vertx redis api
    RedisOptions redisOptions = new RedisOptions()
        .setType(RedisClientType.CLUSTER)
        .setNetClientOptions(new NetClientOptions()
            .setTcpKeepAlive(true)
            .setTrustAll(true)
            .setTcpFastOpen(true)
            .setTcpNoDelay(true));

    String[] uris = redisUri.split(",");
    for (String uri : uris) {
      redisOptions.addConnectionString(uri);
    }

    return redisOptions;
  }
}
