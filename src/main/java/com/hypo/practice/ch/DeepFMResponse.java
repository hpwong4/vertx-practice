package com.hypo.practice.ch;

import lombok.Data;

/**
 * deep fm 预估响应
 *
 * @author hbwang
 */
@Data
public class DeepFMResponse {

  int http_code;

  /**
   * 请求结果状态，succeed 为成功；failed 为失败
   */
  String status;

  /**
   * ctr 预估结果
   */
  Double pred;

  /**
   * 预估耗时
   */
  int pred_cost_ms;

  /**
   * 模型版本号
   */
  String model_version;

  /**
   * 额外信息
   */
  Extra extra;

  public static class Extra {

    FeatureTransformStatistic feature_transform_statistic;

    /**
     * 特征转换统计结果
     */
    static class FeatureTransformStatistic {
      /**
       * 总特征数
       */
      int total_cnt;

      /**
       * 特征转换成功数
       */
      int succeed_cnt;

      /**
       * 特征转换失败数
       */
      int fail_cnt;

      /**
       * 特征转换成功率
       */
      Double succeed_ratio;
    }
  }
}
