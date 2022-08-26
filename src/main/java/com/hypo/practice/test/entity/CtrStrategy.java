package com.hypo.practice.test.entity;

import com.google.common.base.Strings;
import lombok.Data;

/**
 * @author hbwang
 */
@Data
public class CtrStrategy {
  private int id;
  private int groupId;
  private String strategy;
  private int weight;
  private String calibration;

  public boolean invalid() {
    return id < 1 || groupId < 1 || Strings.isNullOrEmpty(strategy) || weight < 1;
  }
}
