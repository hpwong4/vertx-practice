//package com.hypo.practice.test.utils;
//
//import com.iflytek.hermes.ctr.pojo.CtrStrategyGroup;
//import com.iflytek.hermes.share.TableNameEnum;
//import lombok.Getter;
//
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
///**
// * @author hbwang
// */
//public class CacheUtils {
//
//  @Getter
//  private static final ConcurrentMap<String, ConcurrentMap<Object, Object>> CACHE = new ConcurrentHashMap<>();
//
//  /**
//   * 通过 ctr strategy groupId 获取对应策略组内容
//   *
//   * @param groupId ctr strategy groupId
//   * @return CtrStrategyGroup
//   */
//  public static CtrStrategyGroup getCtrStrategyGroup(int groupId) {
//    ConcurrentMap<Object, Object> map = CACHE.get(TableNameEnum.t_ctr_strategy.name());
//    if (map == null || map.isEmpty()) {
//      return null;
//    }
//
//    Object object = map.get(groupId);
//    if (object == null) {
//      return null;
//    }
//
//    return (CtrStrategyGroup) object;
//  }
//}
