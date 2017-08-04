package com.hbase.entity.jdbc;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * jdbc查询条件实体类
 * lf
 * 2017-08-03 10:15:16
 */
public class QueryCondition extends PageCondition implements BaseCondition, Serializable {

    private static final long serialVersionUID = -4772619333004667706L;

    private static final String GROUP_BY = " GROUP BY ";
    private static final String HAVING = " HAVING ";
    private static final String ORDER_BY = " ORDER BY ";
    private static final String LIMIT = " LIMIT ";

    private String order;
    private String group;
    private String having;

    // 其它条件
    private Condition condition;

    private QueryCondition() {
        condition = Condition.create();
    }

    private QueryCondition(int page, int size) {
        super(page, size);
        condition = Condition.create();
    }

    public static QueryCondition create() {
        return new QueryCondition();
    }

    public static QueryCondition create(int page, int size) {
        return new QueryCondition(page, size);
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

    /**
     * 需要in的，用“,”隔开
     *
     * @param field 字段
     * @param value 值
     * @return
     */
    public QueryCondition addIn(String field, String value) {
        condition.addIn(field, value);
        return this;
    }

    /**
     * 需要between的，用“,”隔开
     *
     * @param field 字段
     * @param value 值
     * @return
     */
    public QueryCondition addBetween(String field, String value) {
        condition.addBetween(field, value);
        return this;
    }

    /**
     * 需要字段
     *
     * @param field 字段
     * @param value 值
     * @return
     */
    public QueryCondition addCondition(String field, Object value) {
        condition.addCondition(field, value, Compare.EQUAL);
        return this;
    }

    /**
     * 需要字段
     *
     * @param field 字段
     * @param value 值
     * @return
     */
    public QueryCondition addCondition(String field, Object value, Compare compare) {
        condition.addCondition(field, value, compare);
        return this;
    }

    public Map<String, String> getInMap() {
        return condition.getInMap();
    }

    public Map<String, String> getBetweenMap() {
        return condition.getBetweenMap();
    }

    public Map<ColumnOps, Object> getConditionMap() {
        return condition.getConditionMap();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(group)) {
            stringBuilder.append(GROUP_BY).append(group);
            if (StringUtils.isNotBlank(having)) {
                stringBuilder.append(HAVING).append(having);
            }
        }
        if (StringUtils.isNotBlank(order)) {
            stringBuilder.append(ORDER_BY).append(order);
        }
        stringBuilder.append(LIMIT).append(super.getStart())
                .append(",")
                .append(super.getSize());
        return stringBuilder.toString();
    }
}
