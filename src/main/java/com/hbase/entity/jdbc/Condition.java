package com.hbase.entity.jdbc;

import org.apache.commons.lang.StringUtils;

/**
 * jdbc查询条件实体类
 * 主要是in 和 between
 * lf
 * 2017-08-03 10:15:16
 */
public class Condition {

    private static final String AND = " AND ";

    private String in;
    private String between;

    private Condition() {
    }

    public static Condition create(){
        return new Condition();
    }

    public Condition setIn(String in) {
        this.in = in;
        return this;
    }

    public Condition setBetween(String between) {
        this.between = between;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(in)){
            stringBuilder.append(AND).append(in);
        }
        if (StringUtils.isNotBlank(between)){
            stringBuilder.append(AND).append(between);
        }
        return stringBuilder.toString();
    }
}
