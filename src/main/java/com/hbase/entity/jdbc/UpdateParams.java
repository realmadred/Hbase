package com.hbase.entity.jdbc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class UpdateParams extends Condition implements BaseCondition,Serializable {

    private static final long serialVersionUID = -1275612438007113665L;

    private Map<String,Object> data = new HashMap<>();

    private UpdateParams() {
    }

    public static UpdateParams create(){
        return new UpdateParams();
    }

    /**
     * 条件数据
     * @param field 字段
     * @param value 值
     * @return
     */
    public UpdateParams addData(String field, Object value){
        data.put(field, value);
        return this;
    }

    /**
     * 需要in的，用“,”隔开
     * @param field 字段
     * @param value 值
     * @return
     */
    public UpdateParams addIn(String field, String value) {
        super.addIn(field,value);
        return this;
    }

    /**
     * 需要between的，用“,”隔开
     * @param field 字段
     * @param value 值
     * @return
     */
    public UpdateParams addBetween(String field, String value) {
        super.addBetween(field,value);
        return this;
    }

    /**
     * 需要字段
     * @param field 字段
     * @param value 值
     * @return
     */
    public UpdateParams addCondition(String field, Object value) {
        addCondition(field,value,Compare.EQUAL);
        return this;
    }

    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpdateParams)) return false;
        if (!super.equals(o)) return false;

        UpdateParams that = (UpdateParams) o;

        return getData().equals(that.getData());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getData().hashCode();
        return result;
    }
}
