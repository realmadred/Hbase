package com.hbase.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by lf on 2017-08-03 13:57:52.
 */
public class Common {

    public static String toString(Object obj){
        return obj == null ? "":obj.toString();
    }

    /**
     * 获取map中的字符串值
     * lf
     * 2017-5-25 16:46:45
     * @param map
     * @param key
     * @return
     */
    public static String getMapString(Map<String, List<String>> map, String key){
        if (CollectionUtils.isEmpty(map) ||
                StringUtils.isBlank(key) ||
                !map.containsKey(key)) return "";
        final List<String> list = map.get(key);
        if (CollectionUtils.isEmpty(list)) return "";
        return list.get(0);
    }

    /**
     * 判读对象是否为空
     * lf
     * 2017-5-25 16:41:28
     * @param obj
     * @return
     */
    public static boolean isEmpty(Object obj){
        return StringUtils.isBlank(toString(obj));
    }

    /**
     * 判读是否存在为空的对象
     * lf
     * 2017-5-25 16:41:28
     * @param obj 对象
     * @return
     */
    public static boolean hasEmpty(Object... obj){
        if (obj == null || obj.length == 0) return true;
        for (int i = 0; i < obj.length; i++) {
            if (isEmpty(obj[i])) return true;
        }
        return false;
    }

    /**
     * 是否是数字
     * @param obj 对象
     * @return
     */
    public static boolean isNumber(Object obj){
        return NumberUtils.isNumber(toString(obj));
    }

    /**
     * 获取int值
     * lf
     * 2017-5-25 16:43:52
     * @param obj
     * @return
     */
    public static Integer getInteger(Object obj){
        return !isNumber(obj) ? 0 : Integer.valueOf(obj.toString());
    }

    /**
     * 获取long值
     * lf
     * 2017-5-25 16:43:52
     * @param obj
     * @return
     */
    public static Long getLong(Object obj){
        return !isNumber(obj) ? 0L : Long.valueOf(obj.toString());
    }

    /**
     * 获取double值
     * lf
     * 2017-5-25 16:43:52
     * @param obj
     * @return
     */
    public static double getDouble(Object obj){
        return !isNumber(obj) ? 0.0 : Double.valueOf(obj.toString());
    }

    public static void main(String[] args) {
        System.out.println(hasEmpty(1,2,3));
    }
}
