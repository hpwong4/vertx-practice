package com.hypo.practice.test.vo;

import com.google.common.base.Strings;
import lombok.Data;

import java.util.List;

/**
 * @author hbwang
 */
@Data
public class CtrRequest {

  private String sid;
  private int aim;

  /**
   * 策略组 id
   */
  private int groupId;
  private int adUnitId;
  private int mediaId;
  private String did;
  private List<Ad> ads;

  /**
   * 广告内容
   */
  @Data
  public static class Ad {
    private int platId;
    private List<String> imageUrls;
  }

  /**
   * 请求是否无效校验
   *
   * @return invalid or not
   */
  public boolean invalid() {
    return Strings.isNullOrEmpty(sid) || adUnitId < 1 || mediaId < 1 || Strings.isNullOrEmpty(did) || ads == null || ads.size() < 1;
  }
}
