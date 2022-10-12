package com.hypo.practice.ch;

import lombok.Data;

/**
 * @author hbwang
 */
@Data
public class HermesPredictStatis {

  String platId;
  String modelName;
  String modelVersion;
  String hourStamp;
  int clkStatus;

  long totalCnt;
  double totalCtr;

  long budget01;
  long budget02;
  long budget03;
  long budget04;
  long budget05;
  long budget06;
  long budget07;
  long budget08;
  long budget09;
  long budget10;

  public HermesPredictStatis(String platId, String modelName, String modelVersion, String hourStamp, int clkStatus) {
    this.platId = platId;
    this.modelName = modelName;
    this.modelVersion = modelVersion;
    this.hourStamp = hourStamp;
    this.clkStatus = clkStatus;
  }
}
