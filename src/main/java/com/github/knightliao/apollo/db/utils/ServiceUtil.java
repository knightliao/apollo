package com.github.knightliao.apollo.db.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.knightliao.apollo.db.bo.BaseObject;

/**
 * Service层工具
 *
 * @author liaoqiqi
 * @version 2014-2-20
 */
public class ServiceUtil {

    /**
     * 转换成Map
     *
     * @param enTs
     *
     * @return
     */
    public static <T extends BaseObject<Long>> Map<Long, T> convert2Map(
            List<T> enTs) {

        Map<Long, T> map = new HashMap<Long, T>();

        if (enTs == null) {
            return map;
        }

        for (T t : enTs) {

            map.put(t.getId(), t);
        }

        return map;
    }

    /**
     * 获得List<T> 的ID列表
     *
     * @param enTs
     *
     * @return
     */
    public static <T extends BaseObject<Long>> List<Long> convert2IdList(
            List<T> enTs) {
        List<Long> result = new ArrayList<Long>();

        if (enTs == null) {
            return result;
        }

        for (T t : enTs) {
            result.add(t.getId());
        }

        return result;
    }
}
