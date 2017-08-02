package com.hbase.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * 单个个列族数据
 */
public class HbaseDataOneFamily implements Serializable{

    private static final long serialVersionUID = -3877174591948747668L;
    private String key;
    private Map<String, String> columnMap;

    public HbaseDataOneFamily() {
    }

    public HbaseDataOneFamily(String key, Map<String, String> columnMap) {
        this.key = key;
        this.columnMap = columnMap;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Map<String, String> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(Map<String, String> columnMap) {
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

