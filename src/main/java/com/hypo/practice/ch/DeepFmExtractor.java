package com.hypo.practice.ch;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import com.hypo.practice.ch.hermes.CtrRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import static com.hypo.practice.ch.ClickhouseTest.GSON;
import static com.hypo.practice.ch.ClickhouseTest.logger;
import static com.hypo.practice.ch.ClickhouseTest.getDataSource;

public class DeepFmExtractor {


  private static final Logger aLogger = LoggerFactory.getLogger("ctr-logger");
  public static final Type DEEP_TYPE =  new TypeToken<HashMap<String, DeepFMResult.DeepFMPredictResult>>() {}.getType();


  /**
   * 提取 noDense7d 响应
   *
   * @param dataSource dataSource
   */
  private static void extractNoDense7dResponse(DataSource dataSource) {

    String sql = "select deep_fm from hermes.hermes_predict_log2 where err_code = 0 limit 100000";

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String deepFm = rs.getString("deep_fm");
        if (Strings.isNullOrEmpty(deepFm)) {
          continue;
        }

        HashMap<Integer, DeepFMResult.DeepFMPredictResult> deepFMResultMap = GSON.fromJson(deepFm, DEEP_TYPE);

        if (deepFMResultMap == null || deepFMResultMap.isEmpty()) {
          continue;
        }

        for (DeepFMResult.DeepFMPredictResult result : deepFMResultMap.values()) {
          DeepFMResponse response = result.deepFMResponseHashMap.get("no-dense7d");

          aLogger.info(GSON.toJson(response));
        }
      }
    } catch (Exception e) {
      logger.error("Exception = ", e);
    }
  }

  /**
   * 提取 deep fm 请求
   *
   * @param dataSource dataSource
   */
  private static void extractDeepFMRequest(DataSource dataSource) {

    String sql = "select ctr_request from hermes.hermes_predict_log3 where err_code = 0 limit 10000";

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String ctrRequestTmp = rs.getString("ctr_request");
//        String ctrRequest = rs.getNString(1);
        if (Strings.isNullOrEmpty(ctrRequestTmp)) {
          continue;
        }

        CtrRequest ctrRequest = GSON.fromJson(ctrRequestTmp, CtrRequest.class);
        for (CtrRequest.Ad ad : ctrRequest.ads) {
//          int platId = ad.getPlatId();
          List<String> image = ad.imageUrls;

          HashMap<String, String> features = Maps.newHashMap();
          features.putAll(ctrRequest.features);

          features.put("imgurl", image.get(0));
          features.put("material_height", String.valueOf(ad.height));
          features.put("material_width", String.valueOf(ad.width));
          features.put("material_title", ad.title == null ? "" : ad.title);
          features.put("material_desc", ad.desc == null ? "" : ad.desc);
          features.put("material_action_type", String.valueOf(ad.actionType));

          if (!features.containsKey("device_lan")) {
            features.put("device_lan", "zh-CN");
          }

          if (!features.containsKey("risk_code")) {
            features.put("risk_code", "");
          }

          if (!features.containsKey("risk_score")) {
            features.put("risk_score", "0");
          }

          // os 特征转为 int 取值
          String os = features.get("os");
          if (Strings.isNullOrEmpty(os)) {
            features.put("os", "0");
          } else if (os.equals("android")) {
            features.put("os", "1");
          } else if (os.equals("ios")) {
            features.put("os", "2");
          } else {
            features.put("os", "0");
          }


          aLogger.info(GSON.toJson(features));
        }

//          logger.info(ctrRequestTmp);
//        HashMap<Integer, DeepFMResult.DeepFMPredictResult> deepFMResultMap = GSON.fromJson(deepFm, DEEP_TYPE);
//
//        if (deepFMResultMap == null || deepFMResultMap.isEmpty()) {
//          continue;
//        }
//
//        for (DeepFMResult.DeepFMPredictResult result : deepFMResultMap.values()) {
//          DeepFMResponse response = result.deepFMResponseHashMap.get("no-dense7d");
//
//          aLogger.info(GSON.toJson(response));
//        }
      }
    } catch (Exception e) {
      logger.error("Exception = ", e);
    }
  }

  public static void main(String[] args) {

    ClickHouseDataSource dataSource = getDataSource();
    if (dataSource == null) {
      return;
    }

    // 提取 noDense7d 响应
//    extractNoDense7dResponse(dataSource);

    // 提取 deepFM 请求
    extractDeepFMRequest(dataSource);

  }
}
