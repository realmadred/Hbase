package com.hbase.entity.jdbc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * jdbc查询条件实体类
 * 主要是in 和 between
 * lf
 * 2017-08-03 10:15:16
 */
public class Condition implements BaseCondition,Serializable{

    private static final long serialVersionUID = 905254456195982148L;
    private Map<ColumnOps,Object> conditionMap = new HashMap<>();
    private Map<String,String> inMap = new HashMap<>();
    private Map<String,String> betweenMap = new HashMap<>();

    protected Condition() {
    }

    public static Condition create(){
        return new Condition();
    }

    /**
     * 需要in的，用“,”隔开
     * @param field 字段
     * @param value 值
     * @return
     */
    public Condition addIn(String field, String value) {
        inMap.put(field,value);
        return this;
    }

    /**
     * 需要between的，用“,”隔开
     * @param field 字段
     * @param value 值
     * @return
     */
    public Condition addBetween(String field, String value) {
        betweenMap.put(field,value);
        return this;
    }

    /**
     * 需要字段
     * @param field 字段
     * @param value 值
     * @return
     */
    public Condition addCondition(String field, Object value) {
        addCondition(field,value,Compare.EQUAL);
        return this;
    }

    /**
     * 需要字段
     * @param field 字段
     * @param value 值
     * @return
     */
    public Condition addCondition(String field, Object value,Compare compare) {
        conditionMap.put(new ColumnOps(field,compare),value);
        return this;
    }

    @Override
    public Map<String, String> getInMap() {
        return inMap;
    }

    @Override
    public Map<String, String> getBetweenMap() {
        return betweenMap;
    }

    @Override
    public Map<ColumnOps, Object> getConditionMap() {
        return conditionMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Condition)) return false;

        Condition condition = (Condition) o;

        if (conditionMap != null ? !conditionMap.equals(condition.getConditionMap()) : condition.getConditionMap() != null)
            return false;
        if (inMap != null ? !inMap.equals(condition.getInMap()) : condition.getInMap() != null) return false;
        return betweenMap != null ? betweenMap.equals(condition.getBetweenMap()) : condition.getBetweenMap() == null;
    }

    @Override
    public int hashCode() {
        int result = getConditionMap() != null ? getConditionMap().hashCode() : 0;
        result = 31 * result + (getInMap() != null ? getInMap().hashCode() : 0);
        result = 31 * result + (getBetweenMap() != null ? getBetweenMap().hashCode() : 0);
        return result;
    }
}
