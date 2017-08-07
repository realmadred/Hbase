package com.hbase.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 单个个列族数据
 */
public class HbaseDataOneFamily implements Serializable{

    private static final long serialVersionUID = -3877174591948747668L;
    private String key;
    private Map<String, Object> columnMap = new HashMap<>();

    public HbaseDataOneFamily() {
    }

    public HbaseDataOneFamily(String key, Map<String, Object> columnMap) {
        this.key = key;
        this.columnMap = columnMap;
    }

    public HbaseDataOneFamily putData(String col, Object value){
        columnMap.put(col,value);
        return this;
    }

    public String getKey() {
        return key;
    }

    public HbaseDataOneFamily setKey(String key) {
        this.key = key;
        return this;
    }

    public Map<String, Object> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, Object> columnMap) {
        this.columnMap = columnMap;
    }

    @Override
    public String toString() {
        return "HbaseDataOneFamily{" +
                "key='" + key + '\'' +
                ", columnMap=" + columnMap +
                '}';
    }
}

