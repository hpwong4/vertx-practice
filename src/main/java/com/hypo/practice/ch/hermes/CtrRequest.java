package com.hypo.practice.ch.hermes;

import com.google.common.base.Strings;
import lombok.Data;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;

/**
 * @author hbwang
 */
@Data
public class CtrRequest {

  public String sid;
  public int aim;

  public HashMap<String, String> features;

  /**
   * 策略组 id
   */
  public int groupId;
  public int adUnitId;
  public int mediaId;
  public String did;
  @Getter
  public List<Ad> ads;

  /**
   * 广告内容
   */
  @Data
  public static class Ad {
    public int platId;

    public String title;
    public String desc;
    public int actionType;
    public int height;
    public int width;

    public String imageSize;
    public List<String> imageUrls;
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
