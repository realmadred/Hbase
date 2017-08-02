package com.hbase.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * 单个个列族数据
 */
public class HbaseDeleteEntity implements Serializable {

    private static final long serialVersionUID = 7367191162045070831L;
    private String key;
    private Set<String> columns = new HashSet<>(0);

    public HbaseDeleteEntity() {
    }

    public HbaseDeleteEntity(String key, Set<String> columns) {
        this.key = key;
        this.columns = columns;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }
}

