package com.hbase.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class HBaseResult implements Serializable{

    private static final long serialVersionUID = 5264287449827577805L;
    private String key;
    private String columnFamily;
    private Long timestamp;
    private Map<String,String> results = new HashMap<>();

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getResults() {
        return results;
    }

    public void setResults(Map<String, String> results) {
        this.results = results;
    }

    @Override
    public String toString() {
        return "HBaseResult{" +
                "key='" + key + '\'' +
                ", columnFamily='" + columnFamily + '\'' +
                ", timestamp=" + timestamp +
                ", results=" + results +
                '}';
    }
}
