package com.hypo.practice.ch;

import com.clickhouse.client.*;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hypo.practice.ch.DeepFmExtractor.DEEP_TYPE;

/**
 * @author hbwang
 */
public class ClickhouseTest {

  public static final Logger logger = LoggerFactory.getLogger(ClickhouseTest.class);
  public static final Gson GSON = new Gson();

  private static final Type MAP_TYPE =  new TypeToken<Map<String, Double>>() {}.getType();

  private static void httpChClient() {
    ClickHouseNode node = ClickHouseNode.of("http://clickhouse.m6c.co:80/?user=default&password=clickhouse");

    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.connect(node).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
             .query("select * from hermes.hermes_monitor_log").executeAndWait()) {

      for (ClickHouseRecord r : response.records()) {
        // type conversion
        String str = r.getValue(0).asString();
        System.out.println(str);
      }

    } catch (Exception e) {
      System.out.println("exception = " + e);
    }
  }


  static ClickHouseDataSource getDataSource() {
    String url = "jdbc:ch:http://clickhouse.m6c.co:80";
    Properties properties = new Properties();
    properties.setProperty("user", "default");
    properties.setProperty("password", "clickhouse");
    properties.setProperty("socket_timeout", "3000000");
//    properties.setProperty("client_name", "Agent #1");

    ClickHouseDataSource dataSource = null;
    try {
      dataSource = new ClickHouseDataSource(url, properties);
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }

    return dataSource;
  }

  private static void forwardCtrAccuracyCheck(String sql, double thresholdCTR) {

    ClickHouseDataSource dataSource = getDataSource();
    if (dataSource == null) {
      return;
    }

    // 校准前 & 校准后
    int total = 0;
    int oriUpThreshold = 0;
    int upThreshold = 0;

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        int platId = rs.getInt("win_plat_id");
        String oriStr = rs.getString("ori_ctr");
        if (Strings.isNullOrEmpty(oriStr)) {
          continue;
        }
        String ctrMapStr = rs.getString("ctr_map");
        if (Strings.isNullOrEmpty(ctrMapStr)) {
          continue;
        }

        total++;

        Map<String, Double> oriCtrMap = GSON.fromJson(oriStr, MAP_TYPE);
        Double oriCtr = oriCtrMap.get(String.valueOf(platId));

        Map<String, Double> ctrMap = GSON.fromJson(ctrMapStr, MAP_TYPE);
        Double ctr = ctrMap.get(String.valueOf(platId));

        if (oriCtr != null && oriCtr >= thresholdCTR) {
          oriUpThreshold++;
        }
        if (ctr != null && ctr >= thresholdCTR) {
          upThreshold++;
        }
      }

      logger.info("thresholdCTR = {}, total = {}, ori accuracy = {}", thresholdCTR, total, oriUpThreshold * 1.0 / total);
      logger.info("thresholdCTR = {}, total = {}, ctr accuracy = {}", thresholdCTR, total, upThreshold * 1.0 / total);
    } catch (Exception e) {
      logger.error("Exception = ", e);
    }
  }

  private static void reverseCtrAccuracyCheck(String sql, double thresholdCTR) {

    ClickHouseDataSource dataSource = getDataSource();
    if (dataSource == null) {
      return;
    }

    // 校准前 & 校准后
    int total = 0;
    int oriUnderThreshold = 0;
    int underThreshold = 0;

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        int platId = rs.getInt("win_plat_id");
        String oriStr = rs.getString("ori_ctr");
        if (Strings.isNullOrEmpty(oriStr)) {
          continue;
        }
        String ctrMapStr = rs.getString("ctr_map");
        if (Strings.isNullOrEmpty(ctrMapStr)) {
          continue;
        }

        total++;

        Map<String, Double> oriCtrMap = GSON.fromJson(oriStr, MAP_TYPE);
        Double oriCtr = oriCtrMap.get(String.valueOf(platId));

        Map<String, Double> ctrMap = GSON.fromJson(ctrMapStr, MAP_TYPE);
        Double ctr = ctrMap.get(String.valueOf(platId));

        if (oriCtr != null && oriCtr < thresholdCTR) {
          oriUnderThreshold++;
        }
        if (ctr != null && ctr < thresholdCTR) {
          underThreshold++;
        }
      }

      logger.info("thresholdCTR = {}, total = {}, ori accuracy = {}", thresholdCTR, total, oriUnderThreshold * 1.0 / total);
      logger.info("thresholdCTR = {}, total = {}, ctr accuracy = {}", thresholdCTR, total, underThreshold * 1.0 / total);
    } catch (Exception e) {
      logger.error("Exception = ", e);
    }
  }

  /**
   * 正向检查
   */
  private static void forwardCheck() {
    // 正向准确率，产生点击的会话点击率超过 0.8 的数目占总数目的比例
//    String sql = "SELECT\n" +
//        "\tmonitor.sid,\n" +
//        "\tmonitor.win_plat_id,\n" +
//        "\tmonitor.monitor_type,\n" +
//        "\tpredict.strategy_id,\n" +
//        "\tpredict.ori_ctr,\n" +
//        "\tpredict.ctr_map \n" +
//        "FROM\n" +
//        "\thermes.hermes_monitor_log monitor\n" +
//        "\tLEFT JOIN hermes.hermes_predict_log1 predict ON monitor.sid = predict.sid \n" +
//        "WHERE\n" +
//        "\tmonitor.monitor_type = 2 \n" +
//        "\tAND predict.strategy_id = 14" +
//        "\tAND predict.err_code = 0 and monitor.create_time >= '2022-09-25 00:00:00' and monitor.create_time <= '2022-09-26 12:10:10' ;";

//    String sql = "SELECT\n" +
//        "\tpredict.sid,\n" +
//        "\tmonitor.win_plat_id,\n" +
//        "\tmonitor.monitor_type,\n" +
//        "\tpredict.err_code,\n" +
//        "\tpredict.strategy_id,\n" +
//        "\tpredict.ori_ctr,\n" +
//        "\tpredict.ctr_map \n" +
//        "FROM\n" +
//        "\thermes.hermes_predict_log1 predict\n" +
//        "\tLEFT JOIN hermes.hermes_monitor_log monitor ON monitor.sid = predict.sid \n" +
//        "WHERE\n" +
//        "\tmonitor.create_time >= '2022-09-22 00:00:00' \n" +
//        "\tAND predict.strategy_id = 10\n" +
//        "    AND monitor.create_time <= '2022-09-23 00:10:10' \n" +
//        "    AND monitor.monitor_type = 2\n" +
//        "    AND predict.err_code = 0 \n" +
//        "    AND monitor.sid IN ( SELECT sid FROM hermes.hermes_monitor_log WHERE create_time >= '2022-09-22 00:00:00' AND create_time <= '2022-09-23 00:10:20' AND monitor_type = 2);";

    String sql = "SELECT\n" +
        "\tpredict.sid,\n" +
        "\tmonitor.win_plat_id,\n" +
        "\tmonitor.monitor_type,\n" +
        "\tpredict.err_code,\n" +
        "\tpredict.strategy_id,\n" +
        "\tpredict.ori_ctr,\n" +
        "\tpredict.ctr_map \n" +
        "FROM\n" +
        "\thermes.hermes_predict_log1 predict\n" +
        "\tLEFT JOIN hermes.hermes_monitor_log monitor ON monitor.sid = predict.sid \n" +
        "WHERE\n" +
        "\tmonitor.create_time >= '2022-09-28 17:00:00' \n" +
        "\tAND predict.strategy_id = 14\n" +
        "  AND monitor.create_time <= '2022-09-29 12:10:10' \n" +
        "  AND monitor.monitor_type = 2 ";

//    for (double thresholdCTR = 0.5; thresholdCTR < 1; thresholdCTR = thresholdCTR + 0.1) {
//      forwardCtrAccuracyCheck(sql, thresholdCTR);
//    }
    logger.info(sql);
    check(sql);
  }


  private static void check(String sql) {
    ClickHouseDataSource dataSource = getDataSource();
    if (dataSource == null) {
      return;
    }

    int total = 0;

    double totalOriCtr = 0;
    double totalCtr = 0;

    Map<String, Integer> oriCtrMapTT = Maps.newHashMap();
    Map<String, Integer> ctrMapTT = Maps.newHashMap();

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        int platId = rs.getInt("win_plat_id");
        String oriStr = rs.getString("ori_ctr");
        if (Strings.isNullOrEmpty(oriStr)) {
          continue;
        }
        String ctrMapStr = rs.getString("ctr_map");
        if (Strings.isNullOrEmpty(ctrMapStr)) {
          continue;
        }

        total++;

        Map<String, Double> oriCtrMap = GSON.fromJson(oriStr, MAP_TYPE);
        Double oriCtr = oriCtrMap.get(String.valueOf(platId));

        Map<String, Double> ctrMap = GSON.fromJson(ctrMapStr, MAP_TYPE);
        Double ctr = ctrMap.get(String.valueOf(platId));

        if (oriCtr != null && oriCtr > 0) {
          totalOriCtr += oriCtr;
          String key = new BigDecimal(oriCtr).setScale(1, RoundingMode.DOWN).toString();
          oriCtrMapTT.put(key, oriCtrMapTT.getOrDefault(key, 0) + 1);
        }

        if (ctr != null && ctr > 0) {
          totalCtr += ctr;
          String key = new BigDecimal(ctr).setScale(1, RoundingMode.DOWN).toString();
          ctrMapTT.put(key, ctrMapTT.getOrDefault(key, 0) + 1);
        }
      }
    } catch (Exception e) {
      logger.error("Exception = ", e);
    }

    logger.info("ori average = {}, total = {}, ori map = {}", totalOriCtr / total, total, oriCtrMapTT);
    logger.info("ctr average = {}, total = {}, ctr map = {}", totalCtr / total, total, ctrMapTT);

  }

  private static void reverseCheck() {
    // 有曝光无点击
//    String sql = "SELECT\n" +
//        "\tpredict.sid,\n" +
//        "\tmonitor.win_plat_id,\n" +
//        "\tmonitor.monitor_type,\n" +
//        "\tpredict.err_code,\n" +
//        "\tpredict.strategy_id,\n" +
//        "\tpredict.ori_ctr,\n" +
//        "\tpredict.ctr_map \n" +
//        "FROM\n" +
//        "\thermes.hermes_predict_log1 predict\n" +
//        "\tLEFT JOIN hermes.hermes_monitor_log monitor ON monitor.sid = predict.sid \n" +
//        "WHERE\n" +
//        "\tmonitor.create_time >= '2022-09-22 00:00:00' \n" +
//        "\tAND predict.strategy_id = 10\t\n" +
//        "        AND monitor.create_time <= '2022-09-23 00:10:10' \n" +
//        "        AND monitor.monitor_type = 1 \n" +
//        "        AND predict.err_code = 0 \n" +
//        "        AND monitor.sid NOT IN ( SELECT sid FROM hermes.hermes_monitor_log WHERE create_time >= '2022-09-22 00:00:00' AND create_time <= '2022-09-23 00:10:20' AND monitor_type = 2 \n" +
//        "\t);";

    String sql = "SELECT\n" +
        "\tpredict.sid,\n" +
        "\tmonitor.win_plat_id,\n" +
        "\tmonitor.monitor_type,\n" +
        "\tpredict.err_code,\n" +
        "\tpredict.strategy_id,\n" +
        "\tpredict.ori_ctr,\n" +
        "\tpredict.ctr_map \n" +
        "FROM\n" +
        "\thermes.hermes_predict_log1 predict\n" +
        "\tLEFT JOIN hermes.hermes_monitor_log monitor ON monitor.sid = predict.sid \n" +
        "WHERE\n" +
        "\tmonitor.create_time >= '2022-09-28 17:00:00' \n" +
        "\tAND predict.strategy_id = 14\n" +
        "  AND monitor.create_time <= '2022-09-29 12:10:10' \n" +
        "  AND monitor.monitor_type = 1 ";
//    double[] thresholdCTRs = new double[]{0.5, 0.4, 0.3, 0.2, 0.1};
//
//    for (double thresholdCTR : thresholdCTRs) {
//      reverseCtrAccuracyCheck(sql, thresholdCTR);
//    }

    logger.info(sql);
    check(sql);
  }


  public static void main1(String[] args) {
    forwardCheck();
    reverseCheck();


//    double d = 0.361;
//    System.out.println(new BigDecimal(d).setScale(1, RoundingMode.DOWN).toString());
  }


  private static void deepFMCheck(String sql) {
    ClickHouseDataSource dataSource = getDataSource();
    if (dataSource == null) {
      return;
    }

    int total = 0;


    int v1Cnt = 0;
    int noDenseCnt = 0;
    int noDense7dCnt = 0;

    double v1CtrSum = 0;
    double noDenseSum = 0;
    double noDense7dSum = 0;

    Map<String, Integer> v1BucketMap = Maps.newHashMap();
    Map<String, Integer> noDenseBucketMap = Maps.newHashMap();
    Map<String, Integer> noDense7dBucketMap = Maps.newHashMap();

    try (Connection conn = dataSource.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {

        String platId = rs.getString("win_plat_id");
        String deepFm = rs.getString("deep_fm");
        if (Strings.isNullOrEmpty(deepFm) || Strings.isNullOrEmpty(platId)) {
          continue;
        }

        HashMap<String, DeepFMResult.DeepFMPredictResult> deepFMResultMap = GSON.fromJson(deepFm, DEEP_TYPE);

        if (deepFMResultMap == null || deepFMResultMap.isEmpty()) {
          continue;
        }

        DeepFMResult.DeepFMPredictResult predictResult = deepFMResultMap.get(platId);
        if (predictResult == null || predictResult.deepFMResponseHashMap == null || predictResult.deepFMResponseHashMap.isEmpty()) {
          continue;
        }

        // 总数
        total++;

        HashMap<String, DeepFMResponse> deepFMResponseHashMap = predictResult.deepFMResponseHashMap;
        DeepFMResponse v1 = deepFMResponseHashMap.get("v1");
        if(v1 != null && v1.status.equals("succeed") && v1.pred > 0) {
          v1Cnt++;
          v1CtrSum += v1.pred;
          String key = new BigDecimal(v1.pred).setScale(1, RoundingMode.DOWN).toString();
          v1BucketMap.put(key, v1BucketMap.getOrDefault(key, 0) + 1);
        }

        DeepFMResponse noDense = deepFMResponseHashMap.get("no-dense");
        if(noDense != null && noDense.status.equals("succeed") && noDense.pred > 0) {
          noDenseCnt++;
          noDenseSum += noDense.pred;
          String key = new BigDecimal(noDense.pred).setScale(1, RoundingMode.DOWN).toString();
          noDenseBucketMap.put(key, noDenseBucketMap.getOrDefault(key, 0) + 1);
        }

        DeepFMResponse noDense7d = deepFMResponseHashMap.get("no-dense7d");
        if(noDense7d != null && noDense7d.status.equals("succeed") && noDense7d.pred > 0) {
          noDense7dCnt++;
          noDense7dSum += noDense7d.pred;
          String key = new BigDecimal(noDense7d.pred).setScale(1, RoundingMode.DOWN).toString();
          noDense7dBucketMap.put(key, noDense7dBucketMap.getOrDefault(key, 0) + 1);
        }

      }
    } catch (Exception e) {
      logger.error("Exception = ", e);
    }

    logger.info("deep fm total = {}", total);
    logger.info("v1        total = {}, average = {}, v1 budget map = {}", v1Cnt, v1CtrSum / v1Cnt, v1BucketMap);
    logger.info("noDense   total = {}, average = {}, v1 budget map = {}", noDenseCnt, noDenseSum / noDenseCnt, noDenseBucketMap);
    logger.info("noDense7d total = {}, average = {}, v1 budget map = {}", noDense7dCnt, noDense7dSum / noDense7dCnt, noDense7dBucketMap);
  }

  private static void deepFMClicked(String startTime, String endTime) {
    String sql = "SELECT\n" +
        "\tpredict.sid,\n" +
        "\tmonitor.win_plat_id,\n" +
        "\tmonitor.monitor_type,\n" +
        "\tpredict.err_code,\n" +
        "\tpredict.strategy_id,\n" +
        "\tpredict.deep_fm\n" +
        "FROM\n" +
        "\thermes.hermes_predict_log2 predict\n" +
        "\tLEFT JOIN hermes.hermes_monitor_log monitor ON monitor.sid = predict.sid \n" +
        "WHERE\n" +
        "\tmonitor.create_time >= '__START_TIME__' \n" +
        "\tAND predict.strategy_id = 16\n" +
        "  AND monitor.create_time <= '__END_TIME__' \n" +
        "  AND monitor.monitor_type = 2 ";

    sql = sql.replace("__START_TIME__", startTime).replace("__END_TIME__", endTime);

    logger.info(sql);

    deepFMCheck(sql);
  }

  private static void deepFMImpressedWithoutClick(String startTime, String endTime) {
    String sql = "SELECT\n" +
        "\tpredict.sid,\n" +
        "\tmonitor.win_plat_id,\n" +
        "\tmonitor.monitor_type,\n" +
        "\tpredict.err_code,\n" +
        "\tpredict.strategy_id,\n" +
        "\tpredict.deep_fm \n" +
        "FROM\n" +
        "\thermes.hermes_predict_log2 predict\n" +
        "\tLEFT JOIN hermes.hermes_monitor_log monitor ON monitor.sid = predict.sid \n" +
        "WHERE\n" +
        "\tmonitor.create_time >= '__START_TIME__' \n" +
        "\tAND predict.strategy_id = 16\n" +
        "  AND monitor.create_time <= '__END_TIME__' \n" +
        "  AND monitor.monitor_type = 1 ";

    sql = sql.replace("__START_TIME__", startTime).replace("__END_TIME__", endTime);
    logger.info(sql);

    deepFMCheck(sql);
  }

  public static void main(String[] args) {

    String startTime = "2022-10-08 15:40:00";
    String endTime = "2022-10-08 18:10:10";

    // 有点击
    deepFMClicked(startTime, endTime);

    // 有曝光无点击
    deepFMImpressedWithoutClick(startTime, endTime);
  }


}
