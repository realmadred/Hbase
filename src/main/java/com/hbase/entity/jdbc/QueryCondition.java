package com.hbase.entity.jdbc;

import org.apache.commons.lang.StringUtils;

/**
 * jdbc查询条件实体类
 * lf
 * 2017-08-03 10:15:16
 */
public class QueryCondition extends PageCondition {

    private static final String IN = " AND IN";
    private static final String BETWEEN = " AND BETWEEN";
    private static final String GROUP_BY = " GROUP BY ";
    private static final String HAVING = " HAVING ";
    private static final String ORDER_BY = " ORDER BY ";
    private static final String LIMIT = " LIMIT ";

    private String in;
    private String between;
    private String order;
    private String group;
    private String having;

    private QueryCondition() {
    }

    private QueryCondition(int size, int page) {
        super(size, page);
    }

    public static QueryCondition create(){
        return new QueryCondition();
    }

    public static QueryCondition create(int size, int page){
        return new QueryCondition(size, page);
    }

    public QueryCondition setOrder(String order) {
        this.order = order;
        return this;
    }

    public QueryCondition setGroup(String group) {
        this.group = group;
        return this;
    }

    public QueryCondition setHaving(String having) {
        this.having = having;
        return this;
    }

    public QueryCondition setIn(String in) {
        this.in = in;
        return this;
    }

    public QueryCondition setBetween(String between) {
        this.between = between;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(in)){
            stringBuilder.append(IN).append(in);
        }
        if (StringUtils.isNotBlank(between)){
            stringBuilder.append(BETWEEN).append(between);
        }
        if (StringUtils.isNotBlank(group)){
            stringBuilder.append(GROUP_BY).append(group);
            if (StringUtils.isNotBlank(having)){
                stringBuilder.append(HAVING).append(having);
            }
        }
        if (StringUtils.isNotBlank(order)){
            stringBuilder.append(ORDER_BY).append(order);
        }
        stringBuilder.append(LIMIT).append(super.getStart())
                .append(",")
                .append(super.getSize());
        return stringBuilder.toString();
    }
}
