package com.hbase.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang.StringUtils;

/**
 * 车牌号码省份编码
 * lf
 * 2017-08-07 17:18:24
 */
public class Lpn {

    private static final BiMap<String, String> CODE_PROV = HashBiMap.create(46);

    static {
        CODE_PROV.put("01", "京");
        CODE_PROV.put("02", "津");
        CODE_PROV.put("03", "冀");
        CODE_PROV.put("04", "晋");
        CODE_PROV.put("05", "蒙");
        CODE_PROV.put("06", "辽");
        CODE_PROV.put("07", "吉");
        CODE_PROV.put("08", "黑");
        CODE_PROV.put("09", "沪");
        CODE_PROV.put("10", "苏");
        CODE_PROV.put("11", "浙");
        CODE_PROV.put("12", "皖");
        CODE_PROV.put("13", "闽");
        CODE_PROV.put("14", "赣");
        CODE_PROV.put("15", "鲁");
        CODE_PROV.put("16", "豫");
        CODE_PROV.put("17", "鄂");
        CODE_PROV.put("18", "湘");
        CODE_PROV.put("19", "粤");
        CODE_PROV.put("20", "琼");
        CODE_PROV.put("21", "桂");
        CODE_PROV.put("22", "甘");
        CODE_PROV.put("23", "陕");
        CODE_PROV.put("24", "新");
        CODE_PROV.put("25", "青");
        CODE_PROV.put("26", "宁");
        CODE_PROV.put("27", "渝");
        CODE_PROV.put("28", "川");
        CODE_PROV.put("29", "贵");
        CODE_PROV.put("30", "云");
        CODE_PROV.put("31", "藏");
        CODE_PROV.put("32", "台");
        CODE_PROV.put("33", "澳");
        CODE_PROV.put("34", "港");
    }

    /**
     * 获取省
     *
     * @param code code
     * @return
     */
    public static String getProv(final String code) {
        return CODE_PROV.get(code);
    }

    /**
     * 获取code
     *
     * @param prov 省份简称
     * @return
     */
    public static String getCode(final String prov) {
        return CODE_PROV.inverse().get(prov);
    }

    /**
     * 获取车牌号使用代码
     *
     * @param lpn 车牌
     * @return
     */
    public static String encode(String lpn) {
        if (StringUtils.isBlank(lpn)) return "";
        return getCode(lpn.substring(0, 1)) + lpn.substring(1);
    }

    /**
     * 获取原始车牌号码
     *
     * @param lpn 车牌
     * @return
     */
    public static String decode(String lpn) {
        if (lpn == null || lpn.length() <= 2) return "";
        return getProv(lpn.substring(0, 2)) + lpn.substring(2);
    }

}
