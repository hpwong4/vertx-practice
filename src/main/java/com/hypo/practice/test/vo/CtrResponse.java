package com.hypo.practice.test.vo;

import lombok.Data;

import java.util.HashMap;

/**
 * @author hbwang
 */
@Data
public class CtrResponse {

  int code;
  HashMap<Integer, Double> result;

}
