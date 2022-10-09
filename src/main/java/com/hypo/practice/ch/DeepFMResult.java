package com.hypo.practice.ch;

import lombok.Data;

import java.util.HashMap;

@Data
public class DeepFMResult {

//  HashMap<Integer, DeepFMPredictResult> deepFMResultMap;


  /**
   * deep fm 预估结果
   */
  @Data
  public static class DeepFMPredictResult {
    HashMap<String, DeepFMResponse> deepFMResponseHashMap;
  }
}
